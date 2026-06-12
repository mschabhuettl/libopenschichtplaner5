"""
OPTIONAL FoxPro compound-index (.CDX) WRITER for Schichtplaner5 tables.

This module is OFF BY DEFAULT. The proven, interop-safe behaviour of
``dbf_writer`` is to DELETE a table's stale ``.CDX`` after every write so the
original Schichtplaner5/CodeBase client rebuilds it on next open (Spec D-13/
D-14). The writer here is the opt-in alternative asked for by ROADMAP §B.2: it
rebuilds the ``.CDX`` in place from the current ``.DBF`` contents so the
original can open the table WITHOUT a rebuild. It is enabled only when
``dbf_writer.WRITE_CDX`` is True (env ``SP5_CDX_WRITE=1``); otherwise this
module is never reached.

Format (reverse-engineered byte-for-byte from the sample DB CDX files in the
META repo, cross-checked against the public FoxPro CDX layout)
--------------------------------------------------------------------------
A CDX is a sequence of 512-byte pages. Page 0 is the index header of the
*tag list* B-tree (a B-tree whose keys are tag names → byte offset of that
tag's own index header). Each tag then has its own index header page, an
expression-pool page (the key + FOR expression text), and one or more B-tree
node pages (interior + leaf).

Every Schichtplaner5 main table carries exactly ONE tag named ``ID_TAG`` with
the FOR filter ``.NOT. DELETED()`` and one of two key shapes:

  * single integer key ``ID``  → key length 8, FoxPro *numeric* key (the IEEE
    double of the value, big-endian, high bit flipped so the byte order sorts);
  * composite ``STR(ID,11)+STR(<F>,11)[+STR(<F>,11)]`` → key length 22 or 33,
    *character* key (each integer right-aligned, space-padded, in 11 columns).

Index-header page layout (the bytes this writer emits):
  0x00  uint32   root node byte offset
  0x04  int32    free node list (-1 / 0 here)
  0x08  uint32   key-operation counter, BIG-endian (see "Scope/limits" below)
  0x0C  uint16   key length
  0x0E  uint8    index options: 0x20 compact | 0x40 compound | 0x08 has-FOR
  0x0F  uint8    signature (1)
  0x1F6 uint16   0 = ascending
  0x1F8 uint16   FOR-expression offset in the pool = len(key_expr)+1
  0x1FA uint16   16 (FOR-clause present marker)
  0x1FC uint16   0
  0x1FE uint16   len(key_expr)+1  (duplicate)
The expression pool page right after the header holds
``<key_expr>\\x00<for_expr>\\x00`` zero-padded to 512 bytes.

Node page layout (first 12 bytes are the node header):
  0x00  uint16   attribute: bit0 root, bit1 leaf, bit2 interior
                 (0x03 root+leaf, 0x02 leaf, 0x01 interior)
  0x02  uint16   number of keys on the page
  0x04  int32    left sibling page offset (-1 none)
  0x08  int32    right sibling page offset (-1 none)
Interior nodes then store, uncompressed, ``nkeys`` entries of
``key[keylen] + recno[4 BE] + child_offset[4 BE]``; the separator key is the
LAST key of the child subtree.
Leaf nodes use FoxPro compact compression. After the 12-byte header:
  0x0C  uint16   free bytes
  0x0E  uint32   record-number mask
  0x12  uint8    duplicate-count mask
  0x13  uint8    trailing-count mask
  0x14  uint8    bits used for the record number
  0x15  uint8    bits used for the duplicate count
  0x16  uint8    bits used for the trailing count
  0x17  uint8    bytes per index entry (3)
Each ``entry_bytes`` entry packs ``recno | dup<<recno_bits |
trail<<(recno_bits+dup_bits)``. The (uncompressed) key characters are stored
from the END of the page backwards: each key contributes
``keylen - dup - trail`` fresh bytes, where ``dup`` are shared with the
previous key's prefix and ``trail`` are trimmed trailing pad bytes (0x20 for
character keys, 0x00 for numeric keys). ``recno`` is the 1-based DBF record
number.

Scope / limits (RULE 5 — byte parity)
-------------------------------------
Byte-exact for everything EXCEPT the 4 bytes at 0x08 (the key-operation
counter). That counter is a CodeBase-internal running total of key
insert/delete operations over the table's whole lifetime (e.g. the sample
``5MASHI.CDX`` carries 194 although the table currently has 0 records); it is
NOT derivable from the current table contents and the original does not use it
to read the index. This writer therefore PRESERVES the existing CDX's counter
on an in-place rebuild and falls back to the live record count for a
from-scratch build. Every other byte of the header, expression pool, tag list
and B-tree nodes is reproduced exactly. Verified cases (wine + self round-trip):
single-leaf numeric (``ID``) and composite-character (``STR``) tags, and the
multi-page B-tree (interior + multiple leaves, e.g. ``5HOLID``).
"""

import os
import struct

from .dbf_reader import _dedupe_names, get_table_fields, read_dbf

PAGE = 512

# Fixed offsets that never change for the single-tag SP5 schema.
_TAGLIST_HDR_PAGE = 0  # page 0
_TAGLIST_ROOT_PAGE = 4  # 0x800
_TAG_HDR_PAGE = 2  # 0x400
_TAG_EXPR_PAGE = 3  # 0x600
_TAG_ROOT_PAGE = 5  # 0xa00 — first tag B-tree node

_FOR_EXPR = ".NOT. DELETED()"
_TAG_NAME = "ID_TAG"
_TAGLIST_KEYLEN = 10


# ─── key building ──────────────────────────────────────────────────────────────


def _parse_key_expr(key_expr: str) -> tuple[str, object]:
    """Parse a Schichtplaner5 ``ID_TAG`` / ``-L`` journal key expression.

    Returns one of:
      * ``("numeric", field_name)`` for a bare single numeric field, e.g.
        ``ID`` (main tables) or ``NUMBER`` (the ``-L`` change journals);
      * ``("char", [(field, width), ...])`` for a composite
        ``STR(ID,11)+STR(X,11)[+STR(Y,11)]`` character key.
    Raises ``ValueError`` for anything outside this grammar.
    """
    expr = key_expr.strip()
    if "+" not in expr and "(" not in expr:
        if not expr.isidentifier():
            raise ValueError(f"Unsupported CDX key expression: {expr!r}")
        return ("numeric", expr)
    parts = [p.strip() for p in expr.split("+")]
    out: list[tuple[str, int]] = []
    for p in parts:
        if not (p.startswith("STR(") and p.endswith(")")):
            raise ValueError(f"Unsupported CDX key expression term: {p!r}")
        inner = p[4:-1]
        field, width = inner.split(",")
        out.append((field.strip(), int(width)))
    return ("char", out)


def _numeric_key(value: float) -> bytes:
    """FoxPro 8-byte numeric key: IEEE double, big-endian, sign-folded."""
    b = bytearray(struct.pack(">d", float(value)))
    if b[0] & 0x80:  # negative: invert all bits so they sort below positives
        for i in range(8):
            b[i] ^= 0xFF
    else:  # positive/zero: set the high bit so they sort above negatives
        b[0] |= 0x80
    return bytes(b)


def _char_key(record: dict, terms: list[tuple[str, int]]) -> bytes:
    """Composite character key: each field as right-aligned STR(field,width)."""
    out = b""
    for field, width in terms:
        val = record.get(field)
        ival = int(val or 0)
        out += str(ival).rjust(width).encode("ascii")[:width]
    return out


def _build_keys(filepath: str, key_expr: str) -> tuple[list[tuple[bytes, int]], int, bytes]:
    """Return ``(sorted [(key, recno)], keylen, pad_byte)`` for the live table.

    ``recno`` is the 1-based DBF record number (raw position + 1). Deleted
    records are excluded (the FOR filter is ``.NOT. DELETED()``). Keys are
    returned in ascending sort order, matching how the original walks them.
    """
    fields = get_table_fields(filepath)
    names = _dedupe_names([str(f["name"]) for f in fields])
    kind, spec = _parse_key_expr(key_expr)

    if kind == "numeric":
        keylen, pad = 8, b"\x00"
    else:  # char
        keylen, pad = sum(w for _, w in spec), b"\x20"

    # Read raw records with their physical positions (read_dbf drops deleted
    # rows but also drops the position, so re-read positionally here).
    from .dbf_reader import _parse_record
    from .dbf_writer import _read_header_info

    num_records, header_size, record_size = _read_header_info(filepath)
    keys: list[tuple[bytes, int]] = []
    with open(filepath, "rb") as f:
        f.seek(header_size)
        for raw_idx in range(num_records):
            raw = f.read(record_size)
            if not raw or len(raw) < record_size:
                break
            if raw[0] == 0x2A:  # deleted → excluded by .NOT. DELETED()
                continue
            record = _parse_record(raw, fields, names)
            recno = raw_idx + 1  # CodeBase record numbers are 1-based
            if kind == "numeric":
                key = _numeric_key(record.get(spec) or 0)
            else:
                key = _char_key(record, spec)
            keys.append((key, recno))

    keys.sort(key=lambda kr: (kr[0], kr[1]))
    return keys, keylen, pad


# ─── leaf / interior node encoding ─────────────────────────────────────────────


def _entry_bit_layout(keylen: int) -> tuple[int, int, int, int, int, int]:
    """Return (recno_bits, dup_bits, trail_bits, recno_mask, dup_mask, trail_mask).

    Verified against every sample table: dup/trail bits give ``keylen 8 -> 4,
    22 -> 5, 33 -> 6`` and the record number gets the remaining bits of a
    3-byte entry.
    """
    if keylen <= 15:
        cnt_bits = 4
    elif keylen <= 31:
        cnt_bits = 5
    else:
        cnt_bits = 6
    recno_bits = 24 - 2 * cnt_bits
    recno_mask = (1 << recno_bits) - 1
    cnt_mask = (1 << cnt_bits) - 1
    return recno_bits, cnt_bits, cnt_bits, recno_mask, cnt_mask, cnt_mask


def _compress(key: bytes, prev: bytes, keylen: int, pad: bytes) -> tuple[int, int]:
    """Return ``(dup, trail)`` for *key* relative to *prev*.

    ``dup`` is the shared leading-byte count with the previous key. ``trail`` is
    the number of trailing pad bytes that can be dropped — but ONLY for
    character keys (``pad == b" "``). Numeric (double) keys are never
    trailing-trimmed in the sample files (their trailing 0x00 bytes are kept),
    so ``trail`` stays 0 for them.
    """
    dup = 0
    while dup < keylen and dup < len(prev) and prev[dup] == key[dup]:
        dup += 1
    trail = 0
    if pad == b"\x20":
        while trail < (keylen - dup) and key[keylen - 1 - trail : keylen - trail] == pad:
            trail += 1
    return dup, trail


def _split_into_leaves(
    keys: list[tuple[bytes, int]], keylen: int, pad: bytes
) -> list[list[tuple[bytes, int]]]:
    """Greedily pack keys into leaf pages by actual compressed byte size.

    A leaf holds entries while ``24 + nkeys*3 + chars <= 512``. Compression is
    recomputed at each leaf boundary because the first key of a new leaf shares
    no prefix with the previous leaf's last key (``dup`` resets to 0). This
    reproduces the original's 49/47 split for the 96-key ``5HOLID`` table.
    """
    pages: list[list[tuple[bytes, int]]] = []
    cur: list[tuple[bytes, int]] = []
    chars = 0
    prev = b""
    for key, recno in keys:
        dup, trail = _compress(key, prev, keylen, pad)
        fresh = keylen - dup - trail
        if cur and 24 + (len(cur) + 1) * 3 + chars + fresh > PAGE:
            pages.append(cur)
            cur = []
            prev = b""
            dup, trail = _compress(key, prev, keylen, pad)
            fresh = keylen - dup - trail
            chars = 0
        cur.append((key, recno))
        chars += fresh
        prev = key
    if cur:
        pages.append(cur)
    return pages


def _encode_leaf(
    keys: list[tuple[bytes, int]],
    keylen: int,
    pad: bytes,
    attr: int,
    left: int,
    right: int,
) -> bytes:
    """Encode one compact leaf page from ``keys`` (already sorted)."""
    recno_bits, dup_bits, trail_bits, recno_mask, dup_mask, trail_mask = _entry_bit_layout(
        keylen
    )
    entry_bytes = 3
    nkeys = len(keys)

    entries = bytearray()
    fresh_chunks = []  # per-key fresh bytes, in key order
    prev = b""
    for key, recno in keys:
        dup, trail = _compress(key, prev, keylen, pad)
        fresh = key[dup : keylen - trail]
        val = (
            (recno & recno_mask)
            | ((dup & dup_mask) << recno_bits)
            | ((trail & trail_mask) << (recno_bits + dup_bits))
        )
        entries += val.to_bytes(entry_bytes, "little")
        fresh_chunks.append(fresh)
        prev = key

    # Key chars grow from the END of the page backwards in key order: key0's
    # fresh bytes occupy the very last slot, key1's just before it, etc. So the
    # low→high byte layout is the reversed list of per-key chunks concatenated.
    chars = b"".join(reversed(fresh_chunks))

    page = bytearray(PAGE)
    struct.pack_into("<H", page, 0, attr)
    struct.pack_into("<H", page, 2, nkeys)
    struct.pack_into("<i", page, 4, left)
    struct.pack_into("<i", page, 8, right)

    prologue_end = 24 + len(entries)
    free = PAGE - prologue_end - len(chars)
    struct.pack_into("<H", page, 12, free)
    struct.pack_into("<I", page, 14, recno_mask)
    page[18] = dup_mask
    page[19] = trail_mask
    page[20] = recno_bits
    page[21] = dup_bits
    page[22] = trail_bits
    page[23] = entry_bytes
    page[24 : 24 + len(entries)] = entries
    # key chars grow backwards from the end of the page
    page[PAGE - len(chars) : PAGE] = chars
    return bytes(page)


def _encode_interior(
    separators: list[tuple[bytes, int, int]], keylen: int, attr: int
) -> bytes:
    """Encode an interior node. ``separators`` = [(last_key, last_recno, child_off)]."""
    page = bytearray(PAGE)
    struct.pack_into("<H", page, 0, attr)
    struct.pack_into("<H", page, 2, len(separators))
    struct.pack_into("<i", page, 4, -1)
    struct.pack_into("<i", page, 8, -1)
    off = 12
    for key, recno, child in separators:
        page[off : off + keylen] = key.ljust(keylen, b"\x00")[:keylen]
        off += keylen
        struct.pack_into(">I", page, off, recno)  # recno big-endian
        off += 4
        struct.pack_into(">I", page, off, child)  # child offset big-endian
        off += 4
    return bytes(page)


# ─── index-header pages ────────────────────────────────────────────────────────


def _encode_index_header(
    root_offset: int,
    counter: int,
    keylen: int,
    options: int,
    key_expr: str | None,
) -> bytes:
    """Encode a 512-byte index header page (tag list or a tag)."""
    page = bytearray(PAGE)
    struct.pack_into("<I", page, 0, root_offset)
    struct.pack_into("<i", page, 4, 0)  # free node list
    struct.pack_into(">I", page, 8, counter)  # key-op counter, big-endian
    struct.pack_into("<H", page, 12, keylen)
    page[14] = options
    page[15] = 1  # signature
    if key_expr is not None:
        klen1 = len(key_expr) + 1
        struct.pack_into("<H", page, 0x1F6, 0)
        struct.pack_into("<H", page, 0x1F8, klen1)
        struct.pack_into("<H", page, 0x1FA, 16)
        struct.pack_into("<H", page, 0x1FC, 0)
        struct.pack_into("<H", page, 0x1FE, klen1)
    else:
        # Tag-list header trailer (constant across all sample files).
        struct.pack_into("<H", page, 0x1F6, 0)
        struct.pack_into("<H", page, 0x1F8, 1)
        struct.pack_into("<H", page, 0x1FA, 1)
        struct.pack_into("<H", page, 0x1FC, 0)
        struct.pack_into("<H", page, 0x1FE, 1)
    return bytes(page)


def _encode_taglist_leaf(tag_name: str) -> bytes:
    """The tag-list root leaf: one key *tag_name* → tag header offset 0x400.

    A compact leaf with a single entry pointing at the tag header (page 2).
    Tag names are right-padded with spaces to the 10-byte tag-list key length;
    the trailing pad is compressed via ``trail`` exactly like data leaves, so
    the 6-char ``ID_TAG`` (trail 4) and the 10-char ``NUMBER_TAG`` (trail 0)
    are both reproduced byte-exactly.
    """
    name = tag_name.encode("ascii")[:_TAGLIST_KEYLEN]
    fresh = len(name)
    trail = _TAGLIST_KEYLEN - fresh  # spaces compressed off the end

    page = bytearray(PAGE)
    struct.pack_into("<H", page, 0, 0x0003)  # root+leaf
    struct.pack_into("<H", page, 2, 1)  # 1 key
    struct.pack_into("<i", page, 4, -1)
    struct.pack_into("<i", page, 8, -1)
    free = PAGE - (24 + 3) - fresh
    struct.pack_into("<H", page, 12, free)
    struct.pack_into("<I", page, 14, 0xFFFF)  # recno mask (16 bits)
    page[18] = 0x0F
    page[19] = 0x0F
    page[20] = 0x10  # 16 recno bits
    page[21] = 0x04
    page[22] = 0x04
    page[23] = 0x03  # entry bytes
    # entry: recno = tag header byte offset (0x400 = page 2), dup=0, trail.
    val = (_TAG_HDR_PAGE * PAGE) | (0 << 16) | (trail << 20)
    page[24:27] = val.to_bytes(3, "little")
    # tag-name fresh chars at the very end of the page
    page[PAGE - fresh : PAGE] = name
    return bytes(page)


# ─── B-tree assembly ───────────────────────────────────────────────────────────


def _build_tag_btree(
    keys: list[tuple[bytes, int]], keylen: int, pad: bytes
) -> tuple[list[bytes], int]:
    """Build the tag B-tree node pages.

    Returns ``(pages, root_page_index)`` where ``pages[0]`` is placed at
    ``_TAG_ROOT_PAGE`` and the rest follow. For data that fits one page a
    single root+leaf is emitted; otherwise leaves + one interior root.
    """
    chunks = _split_into_leaves(keys, keylen, pad)
    if len(chunks) <= 1:
        # Single root+leaf page.
        leaf = _encode_leaf(keys, keylen, pad, attr=0x0003, left=-1, right=-1)
        return [leaf], _TAG_ROOT_PAGE

    # Split into leaf pages, then build one interior root over them.
    # Leaf pages occupy _TAG_ROOT_PAGE, _TAG_ROOT_PAGE+1, ...; the interior
    # root is appended LAST (matching the sample 5HOLID, where the root is the
    # final page). Sibling links connect leaves left↔right.
    n_leaves = len(chunks)
    leaf_start = _TAG_ROOT_PAGE
    root_page = leaf_start + n_leaves
    pages: list[bytes] = []
    separators: list[tuple[bytes, int, int]] = []
    for i, chunk in enumerate(chunks):
        page_idx = leaf_start + i
        left = (page_idx - 1) * PAGE if i > 0 else -1
        right = (page_idx + 1) * PAGE if i < n_leaves - 1 else -1
        pages.append(_encode_leaf(chunk, keylen, pad, attr=0x0002, left=left, right=right))
        last_key, last_recno = chunk[-1]
        separators.append((last_key, last_recno, page_idx * PAGE))
    interior = _encode_interior(separators, keylen, attr=0x0001)
    pages.append(interior)
    return pages, root_page


def build_cdx_bytes(
    filepath: str, key_expr: str, counter: int, tag_name: str = _TAG_NAME
) -> bytes:
    """Build the full ``.CDX`` byte image for *filepath*'s single tag.

    *counter* is written into header byte 0x08 (the CodeBase key-op counter);
    callers pass the preserved original value or a fallback. *tag_name* is the
    tag's name (``ID_TAG`` for main tables, ``NUMBER_TAG`` for ``-L`` journals).
    """
    keys, keylen, pad = _build_keys(filepath, key_expr)
    options = 0x68  # has-FOR | compact | compound

    tag_pages, tag_root_page = _build_tag_btree(keys, keylen, pad)

    # Page 0: tag-list index header (root = tag-list leaf at page 4).
    taglist_hdr = _encode_index_header(
        _TAGLIST_ROOT_PAGE * PAGE, counter, _TAGLIST_KEYLEN, options=0xE0, key_expr=None
    )
    # Page 1: unused (all zero) — present in every sample file.
    page1 = b"\x00" * PAGE
    # Page 2: the tag's index header (root = tag B-tree root page). The
    # key-op counter lives only in the tag-list header (page 0); the tag
    # header always carries 0 in every sample file.
    tag_hdr = _encode_index_header(
        tag_root_page * PAGE, 0, keylen, options=options, key_expr=key_expr
    )
    # Page 3: expression pool.
    expr_pool = bytearray(PAGE)
    blob = key_expr.encode("ascii") + b"\x00" + _FOR_EXPR.encode("ascii") + b"\x00"
    expr_pool[: len(blob)] = blob
    # Page 4: tag-list root leaf.
    taglist_leaf = _encode_taglist_leaf(tag_name)

    pages = [taglist_hdr, page1, tag_hdr, bytes(expr_pool), taglist_leaf]
    pages += tag_pages
    return b"".join(pages)


# ─── public entry point ────────────────────────────────────────────────────────


def _read_counter(cdx_path: str, default: int) -> int:
    """Return the existing CDX's key-op counter (header byte 0x08, BE), or *default*."""
    try:
        with open(cdx_path, "rb") as f:
            head = f.read(12)
        if len(head) >= 12:
            return struct.unpack_from(">I", head, 8)[0]
    except OSError:
        pass
    return default


def write_cdx(filepath: str) -> str | None:
    """Rebuild ``<table>.CDX`` in place from *filepath*'s current contents.

    Supports the single-tag schema used by every Schichtplaner5 table
    (``ID_TAG``) and ``-L`` journal (``NUMBER_TAG``). The tag name, key
    expression and key-op counter are read back from the existing CDX so the
    rebuild always matches whatever tag the original created. Returns the
    written CDX path, or ``None`` if there is no CDX / the key expression could
    not be determined (the caller then falls back to deleting the stale CDX).
    Raises ``ValueError`` for an unsupported key shape.
    """
    base, _ = os.path.splitext(filepath)
    cdx_path = base + ".CDX"
    if not os.path.exists(cdx_path):
        # Try a lowercase variant; if there is no CDX at all there is nothing
        # the original expects us to maintain — signal "no CDX".
        if os.path.exists(base + ".cdx"):
            cdx_path = base + ".cdx"
        else:
            return None

    key_expr = _read_key_expr(cdx_path)
    if key_expr is None:
        return None
    tag_name = _read_tag_name(cdx_path) or _TAG_NAME

    num_records = len(read_dbf(filepath))
    counter = _read_counter(cdx_path, default=num_records)
    data = build_cdx_bytes(filepath, key_expr, counter, tag_name)

    tmp = cdx_path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, cdx_path)
    return cdx_path


def _read_tag_name(cdx_path: str) -> str | None:
    """Read the tag name from the existing CDX's tag-list leaf."""
    try:
        with open(cdx_path, "rb") as f:
            data = f.read()
    except OSError:
        return None
    if len(data) < (_TAGLIST_ROOT_PAGE + 1) * PAGE:
        return None
    taglist_root = struct.unpack_from("<I", data, 0)[0]
    leaf = data[taglist_root : taglist_root + PAGE]
    if len(leaf) < PAGE or struct.unpack_from("<H", leaf, 2)[0] < 1:
        return None
    recno_bits = leaf[20]
    dup_mask = leaf[18]
    trail_mask = leaf[19]
    dup_bits = leaf[21]
    entry_bytes = leaf[23] or 3
    val = int.from_bytes(leaf[24 : 24 + entry_bytes], "little")
    dup = (val >> recno_bits) & dup_mask
    trail = (val >> (recno_bits + dup_bits)) & trail_mask
    fresh = _TAGLIST_KEYLEN - dup - trail
    name = leaf[PAGE - fresh : PAGE].rstrip(b"\x00\x20")
    return name.decode("ascii", errors="replace") or None


def _read_key_expr(cdx_path: str) -> str | None:
    """Read the existing CDX's ``ID_TAG`` key expression from its expr pool.

    The expression text is the most reliable source of the live key shape
    (the original may use a key we have not catalogued); reading it back means
    the rebuild always matches whatever tag the original created.
    """
    try:
        with open(cdx_path, "rb") as f:
            data = f.read()
    except OSError:
        return None
    if len(data) < _TAGLIST_ROOT_PAGE * PAGE + PAGE:
        return None
    # tag-list leaf → tag header offset → key length; expr pool sits in the
    # page right after the tag header. For the SP5 single-tag schema the tag
    # header is page 2 and its expression pool page 3.
    taglist_root = struct.unpack_from("<I", data, 0)[0]
    leaf = data[taglist_root : taglist_root + PAGE]
    if len(leaf) < PAGE:
        return None
    recno_mask = struct.unpack_from("<I", leaf, 14)[0]
    entry_bytes = leaf[23] or 3
    if struct.unpack_from("<H", leaf, 2)[0] < 1:
        return None
    tag_hdr_off = int.from_bytes(leaf[24 : 24 + entry_bytes], "little") & recno_mask
    expr_off = tag_hdr_off + PAGE  # expression pool page follows the tag header
    blob = data[expr_off : expr_off + PAGE]
    key_expr = blob.split(b"\x00", 1)[0].decode("ascii", errors="replace")
    return key_expr or None
