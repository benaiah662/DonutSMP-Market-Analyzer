"""ASCII market dashboard. ALL content lives in dashboard_data.txt.

Controls:
    UP/DOWN : switch between the two graph slots
    g       : cycle the selected graph's type (Profit / Vol / any you add)
    t       : cycle the selected graph's period (set by 'periods:' in the data file)
    ?       : help        q : quit

Changing the period changes the graph AND the tables/footer underneath it.
Edit dashboard_data.txt to change anything (including 'width:'); this file holds no data.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
from contextlib import contextmanager
from dataclasses import dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator

DATA_FILE = Path(__file__).with_name("dashboard_data.txt")

DEFAULT_WIDTH = 130             # overridden by 'width:' in the data file
MIN_WIDTH, MAX_WIDTH = 100, 300
GUTTER = 10                     # left axis column: '|' + 8 chars + '|'
DEFAULT_AXIS = ("Date:", tuple(f"{d:02d}" for d in range(1, 31)), 1)
MAX_VALUE = 120
LOW_W = 13                      # every table value column ("14 Aug 2200", "Weekly Low", ...)
MIN_NAME_W = 20                 # the item-name column never gets narrower than this
WIDTH = INNER = PLOT_W = 0
SEPARATOR = ""


def set_width(width: int) -> None:
    """Derive every layout measurement from the one 'width' setting.

    Graph plot area = width - GUTTER (pick a width where that divides nicely,
    e.g. 130 -> 120). Table name column = whatever the value columns leave over.
    """
    global WIDTH, INNER, PLOT_W, SEPARATOR
    WIDTH, INNER, PLOT_W = width, width - 2, width - GUTTER
    SEPARATOR = "=" * width


def name_width(columns: int) -> int:
    """Width of the item-name column for a table with `columns` value columns."""
    return WIDTH - 2 - columns * (LOW_W + 1)


set_width(DEFAULT_WIDTH)


# --------------------------------------------------------------------------- #
# Data loading (once, validated, stored as immutable tuples)
# --------------------------------------------------------------------------- #
class DataError(RuntimeError):
    pass


def _need(condition: bool, message: str) -> None:
    if not condition:
        raise DataError(f"{DATA_FILE.name}: {message}")


def _fail(line_no: int, message: str) -> None:
    raise DataError(f"{DATA_FILE.name} line {line_no}: {message}")


def parse_text(text: str) -> dict[str, Any]:
    """Turn the plain-text data file into the structure validated below."""
    payload: dict[str, Any] = {"title": None, "width": None, "periods": None, "graphs": {}, "tables": {}, "axes": {}}
    graph: list[float] | None = None    # numbers of the current [graph ...] block
    table: dict[str, Any] | None = None  # current [table ...] block
    section: dict[str, Any] | None = None
    axis: dict[str, Any] | None = None   # current [axis ...] block

    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        head = re.fullmatch(r"\[(graph|table|axis)\s+(.+?)\]", line, re.IGNORECASE)
        if head:
            graph = table = section = axis = None
            kind = head.group(1).lower()
            if kind == "axis":
                axis = payload["axes"].setdefault(head.group(2), {})
            elif kind == "graph":
                parts = head.group(2).rsplit(None, 1)
                if len(parts) != 2:
                    _fail(n, "use [graph <Graph Type Name> <period>], e.g. [graph Vol Graph 7d]")
                gtype, period = parts
                graph = payload["graphs"].setdefault(gtype, {}).setdefault(period, [])
            else:
                table = payload["tables"].setdefault(head.group(2), {"sections": [], "footer": None})
            continue

        key, _, value = line.partition(":")
        key, value = key.strip().lower(), value.strip()

        if graph is not None:
            try:
                graph.extend(float(tok) for tok in line.replace(",", " ").split())
            except ValueError:
                _fail(n, f"graph lines may only contain numbers, got: {line!r}")
        elif table is not None:
            if key == "section":
                section = {"title": value, "columns": None, "rows": []}
                table["sections"].append(section)
            elif key == "columns" and section is not None:
                cells = [c.strip() for c in value.split("|")]
                section["columns"] = cells[1:] if len(cells) > 1 and cells[0] == "" else cells
            elif key == "footer":
                table["footer"] = value
            elif section is not None and section["columns"] is not None and "|" in line:
                section["rows"].append([c.strip() for c in line.split("|")])
            else:
                _fail(n, f"don't understand: {line!r} (rows need 'section:' and 'columns:' above them)")
        elif axis is not None:
            if key == "label":
                axis["label"] = value
            elif key == "points":
                axis["points"] = value.replace(",", " ").split()
            elif key == "snapshots":
                value = value.split("#")[0].strip()      # allow a trailing note
                if not value.isdigit() or int(value) < 1:
                    _fail(n, "snapshots: must be a whole number of 1 or more")
                axis["snapshots"] = int(value)
            else:
                _fail(n, f"axis blocks only take 'label:', 'points:' and 'snapshots:', got: {line!r}")
        elif key == "title":
            payload["title"] = value
        elif key == "width":
            value = value.split("#")[0].strip()
            if not value.isdigit():
                _fail(n, "width: must be a whole number of characters, e.g. width: 130")
            payload["width"] = int(value)
        elif key == "periods":
            payload["periods"] = [p.strip() for p in value.split(",") if p.strip()]
        else:
            _fail(n, f"don't understand: {line!r}")

    for block in payload["tables"].values():
        for sec in block["sections"]:
            if sec["columns"] is None:
                raise DataError(f"{DATA_FILE.name}: section '{sec['title']}' has no 'columns:' line")
    return payload


def load_data() -> dict[str, Any]:
    try:
        payload = parse_text(DATA_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise DataError(f"Missing {DATA_FILE.name} (keep it next to this script).") from None

    _need(isinstance(payload, dict), "top level must be an object")
    title, periods = payload.get("title"), payload.get("periods")
    _need(isinstance(title, str), "'title' must be a string")
    _need(isinstance(periods, list) and periods and all(isinstance(p, str) for p in periods),
          "'periods' must be a non-empty list of strings")

    width = payload.get("width") or DEFAULT_WIDTH
    _need(MIN_WIDTH <= width <= MAX_WIDTH, f"width must be between {MIN_WIDTH} and {MAX_WIDTH}")
    set_width(width)
    _need(len(title) + 18 <= WIDTH, f"title is too long for a width of {WIDTH} (max {WIDTH - 18} characters)")

    axes: dict[str, tuple[str, tuple[str, ...], int]] = {}
    raw_axes = payload.get("axes", {})
    for period in periods:
        raw = raw_axes.get(period)
        if raw is None:
            axes[period] = DEFAULT_AXIS
            continue
        label, points = raw.get("label"), raw.get("points")
        _need(isinstance(label, str) and 3 <= len(label) <= GUTTER - 2, f"axis '{period}' needs a 'label:' of 3-{GUTTER - 2} characters")
        _need(isinstance(points, list) and points, f"axis '{period}' needs a 'points:' list")
        narrowest = PLOT_W // len(points)
        _need(narrowest >= 3, f"axis '{period}' has too many points ({len(points)}) for its label")
        _need(all(len(p) <= narrowest - 1 for p in points),
              f"axis '{period}': a point name is longer than {narrowest - 1} characters")
        snaps = raw.get("snapshots", 1)
        _need(narrowest - 1 >= snaps, f"axis '{period}': {snaps} snapshots per point don't fit in {narrowest - 1} characters")
        axes[period] = (label, tuple(points), snaps)

    graphs: dict[str, dict[str, tuple[int, ...]]] = {}
    raw_graphs = payload.get("graphs")
    _need(isinstance(raw_graphs, dict) and raw_graphs, "'graphs' must be a non-empty object")
    for gtype, by_period in raw_graphs.items():
        _need(isinstance(by_period, dict), f"graph '{gtype}' must map period -> values")
        graphs[gtype] = {}
        for period in periods:
            values = by_period.get(period)
            a_label, a_points, a_snaps = axes[period]
            count = len(a_points) * a_snaps
            per = f" x {a_snaps} snapshots each" if a_snaps > 1 else ""
            _need(isinstance(values, list) and len(values) == count,
                  f"graph '{gtype}' / '{period}' needs exactly {count} values ({len(a_points)} {a_label}{per})")
            for i, v in enumerate(values, 1):
                _need(not isinstance(v, bool) and isinstance(v, (int, float)) and 0 <= v <= MAX_VALUE,
                      f"graph '{gtype}' / '{period}' value {i} must be a number 0-{MAX_VALUE}")
            graphs[gtype][period] = tuple(int(round(v)) for v in values)

    tables: dict[str, dict[str, Any]] = {}
    raw_tables = payload.get("tables")
    _need(isinstance(raw_tables, dict), "'tables' must be an object")
    for period in periods:
        block = raw_tables.get(period)
        _need(isinstance(block, dict), f"tables['{period}'] is missing")
        footer = block.get("footer")
        _need(isinstance(footer, str) and len(footer) <= INNER, f"tables['{period}'] footer invalid/too long")
        sections = []
        for s_i, sec in enumerate(block.get("sections", []), 1):
            where = f"tables['{period}'] section {s_i}"
            cols, rows, stitle = sec.get("columns"), sec.get("rows"), sec.get("title")
            _need(isinstance(stitle, str) and len(stitle) <= INNER, f"{where} needs a title that fits")
            _need(isinstance(cols, list) and len(cols) >= 1, f"{where} needs at least one column")
            name_w = name_width(len(cols))
            _need(name_w >= MIN_NAME_W, f"{where}: {len(cols)} columns is too many for width {WIDTH} "
                                        f"(max {(WIDTH - 2 - MIN_NAME_W) // (LOW_W + 1)})")
            _need(all(len(str(c)) <= LOW_W for c in cols), f"{where} column name longer than {LOW_W} characters")
            _need(isinstance(rows, list) and rows, f"{where} needs rows")
            for r in rows:
                _need(isinstance(r, list) and len(r) == len(cols) + 1,
                      f"{where}: each row needs a name + {len(cols)} values separated by |")
                _need(len(str(r[0])) <= name_w, f"{where}: name '{r[0]}' longer than {name_w} characters")
                _need(all(len(str(c)) <= LOW_W for c in r[1:]), f"{where}: value longer than {LOW_W} characters in '{r[0]}'")
            sections.append((stitle, tuple(map(str, cols)), tuple(tuple(map(str, r)) for r in rows)))
        tables[period] = {"sections": tuple(sections), "footer": footer}

    return {"title": title, "width": width, "periods": tuple(periods), "graphs": graphs, "tables": tables, "axes": axes}


try:
    DATA = load_data()
except DataError as error:
    print(f"Dashboard data error: {error}", file=sys.stderr)
    raise SystemExit(1)

GRAPH_TYPES = tuple(DATA["graphs"])
PERIODS = DATA["periods"]


# --------------------------------------------------------------------------- #
# Rendering (pure + cached: each graph/period combo is built exactly once)
# --------------------------------------------------------------------------- #
def boxed(text: str) -> str:
    return "|" + text.center(INNER) + "|"


def table_line(name: str, cells: tuple[str, ...], name_w: int) -> str:
    return "|" + name.ljust(name_w) + "".join("|" + c.center(LOW_W) for c in cells) + "|"


@lru_cache(maxsize=None)
def render_top() -> str:
    left, right = f"|- {DATA['title']} ", " [q] [?] -|"
    return left + "-" * (WIDTH - len(left) - len(right)) + right


def spread(total: int, count: int) -> list[int]:
    """Split `total` characters into `count` near-equal parts, leftovers spread evenly."""
    base, extra = divmod(total, count)
    return [base + ((i + 1) * extra // count - i * extra // count) for i in range(count)]


@lru_cache(maxsize=None)
def render_graph(gtype: str, period: str) -> str:
    values = DATA["graphs"][gtype][period]
    label, points, snaps = DATA["axes"][period]
    widths = spread(PLOT_W, len(points))

    title = f"|      **{gtype} - {period}**"
    controls = "Change Graph Type: [g] ~ Time Period: [t] |"
    lines = [title + " " * max(1, WIDTH - len(title) - len(controls)) + controls]
    # Bars fill their whole cell: the ONLY blank column left is the single
    # 1-character gap between points. Snapshots inside a point touch each other.
    bar_widths = [spread(width - 1, snaps) for width in widths]
    for level in range(MAX_VALUE, 0, -10):
        cells = []
        for idx, widths_in_cell in enumerate(bar_widths):
            chunk = values[idx * snaps:(idx + 1) * snaps]
            cells.append("".join(("#" if v >= level else " ") * bw
                                 for v, bw in zip(chunk, widths_in_cell)) + " ")
        lines.append("|" + str(level).rjust(GUTTER - 2) + "|" + "".join(cells)[:-1] + "|")
    lines.append("|" + "-" * INNER + "|")
    lines.append("|" + label.center(GUTTER - 2) + "|" + "|".join(p.center(w - 1) for p, w in zip(points, widths)) + "|")
    return "\n".join(lines)


@lru_cache(maxsize=None)
def render_tables(period: str) -> str:
    block = DATA["tables"][period]
    lines: list[str] = []
    for title, columns, rows in block["sections"]:
        lines.append(boxed(f"**{title}**"))
        name_w = name_width(len(columns))
        lines.append(table_line("-" * name_w, columns, name_w))
        lines.extend(table_line(r[0], r[1:], name_w) for r in rows)
        lines.append(SEPARATOR)
    lines.append(boxed(block["footer"]))
    lines.append(SEPARATOR)
    return "\n".join(lines)


@lru_cache(maxsize=None)
def render_frame(gtype: str, period: str) -> str:
    frame = "\n".join((render_top(), SEPARATOR, render_graph(gtype, period),
                       SEPARATOR, render_tables(period)))
    bad = [l for l in frame.splitlines() if len(l) != WIDTH]
    if bad:
        raise DataError(f"Rendered line is not {WIDTH} wide: {bad[0]!r}")
    return frame


# --------------------------------------------------------------------------- #
# State (immutable; every key press produces a new state)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Slot:
    gtype: str
    period: str


@dataclass(frozen=True)
class State:
    slots: tuple[Slot, Slot] = (Slot("Profit Graph", "30d"), Slot("Vol Graph", "90d"))
    selected: int = 1


def cycle(options: tuple[str, ...], current: str) -> str:
    return options[(options.index(current) + 1) % len(options)]


def step(state: State, key: str) -> State:
    slot = state.slots[state.selected]

    def with_slot(new: Slot) -> State:
        slots = list(state.slots)
        slots[state.selected] = new
        return replace(state, slots=tuple(slots))

    if key == "UP":
        return replace(state, selected=(state.selected - 1) % len(state.slots))
    if key == "DOWN":
        return replace(state, selected=(state.selected + 1) % len(state.slots))
    if key == "g":
        return with_slot(replace(slot, gtype=cycle(GRAPH_TYPES, slot.gtype)))
    if key == "t":
        return with_slot(replace(slot, period=cycle(PERIODS, slot.period)))
    return state


# --------------------------------------------------------------------------- #
# Terminal I/O
# --------------------------------------------------------------------------- #
CLEAR_ALL = "\033[3J\033[2J\033[H"
HOME = "\033[H"


def enable_vt_windows() -> None:
    if os.name != "nt":
        return
    import ctypes
    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    handle = kernel32.GetStdHandle(-11)
    mode = ctypes.c_uint()
    if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
        kernel32.SetConsoleMode(handle, mode.value | 0x0004)


@contextmanager
def terminal_session() -> Iterator[None]:
    """Hide cursor + put terminal in single-key mode ONCE for the whole session."""
    enable_vt_windows()
    old = None
    if os.name != "nt":
        import termios, tty
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        tty.setcbreak(fd)  # single keys, no echo, Ctrl+C still works
    sys.stdout.write(CLEAR_ALL + "\033[?25l")
    sys.stdout.flush()
    try:
        yield
    finally:
        if old is not None:
            import termios
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old)
        sys.stdout.write(CLEAR_ALL + "\033[?25h")
        sys.stdout.flush()


def read_key() -> str:
    if os.name == "nt":
        import msvcrt
        ch = msvcrt.getwch()
        if ch in ("\x00", "\xe0"):
            return {"H": "UP", "P": "DOWN"}.get(msvcrt.getwch(), "")
        return ch.lower()

    import select
    fd = sys.stdin.fileno()
    ch = os.read(fd, 1).decode(errors="ignore")
    if ch == "\x1b":
        if select.select([fd], [], [], 0.05)[0]:
            return {"[A": "UP", "[B": "DOWN"}.get(os.read(fd, 2).decode(errors="ignore"), "")
        return ""
    return ch.lower()


def draw(state: State) -> bool:
    """Home the cursor and overwrite in ONE write: no flicker, no scrollback spam.

    Returns False (and shows a hint instead) if the terminal is too narrow.
    """
    columns = shutil.get_terminal_size().columns
    if columns < WIDTH:
        sys.stdout.write(
            CLEAR_ALL
            + f"Terminal is {columns} columns wide; the dashboard needs {WIDTH}.\n"
            "Widen or maximise the window (or lower 'width:' in dashboard_data.txt),\n"
            "then press any key.\n"
        )
        sys.stdout.flush()
        return False
    slot = state.slots[state.selected]
    frame = render_frame(slot.gtype, slot.period).replace("\n", "\033[K\n")
    sys.stdout.write(HOME + frame + "\033[K\n\033[J")
    sys.stdout.flush()
    return True


def show_help() -> None:
    sys.stdout.write(
        CLEAR_ALL
        + "DASHBOARD CONTROLS\n\n"
        "  UP / DOWN  Switch between the two graph slots\n"
        f"  g          Cycle graph type: {' / '.join(GRAPH_TYPES)}\n"
        f"  t          Cycle period: {' / '.join(PERIODS)} (graph + tables change)\n"
        "  ?          Show this help\n"
        "  q          Quit\n\n"
        "Press any key to return...\n"
    )
    sys.stdout.flush()
    read_key()


# --------------------------------------------------------------------------- #
# Self-calling main loop
# --------------------------------------------------------------------------- #
def run(state: State, redraw: bool = True) -> None:
    """Draw, wait for a key, then call itself with the next state."""
    fitted = draw(state) if redraw else True
    key = read_key()
    if key == "q":
        return
    if key == "?":
        show_help()
        return run(state)
    new_state = step(state, key)
    return run(new_state, redraw=new_state != state or not fitted)


def main() -> None:
    # Each key press is one stack frame; pure-Python frames are cheap, so this
    # allows tens of thousands of key presses per session.
    sys.setrecursionlimit(50_000)
    with terminal_session():
        run(State())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
