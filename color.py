import math
from typing import Tuple

def hex_color(col: Tuple[int, int, int]):
    """Converts a [r, g, b] tuple to it's hex representation. Transparency is ignored"""
    def h(c): return hex(int(c))[2:].zfill(2)
    return f'#{h(col[0])}{h(col[1])}{h(col[2])}'

def is_transparent(col: Tuple[int, int, int, int], threshold=1):
    return col[3] < 255 * threshold

def color_length(color: Tuple[int, int, int]):
    """Gets the length of the color. Ignores transparency"""
    return math.sqrt(color[0]**2 + color[1]**2 + color[2]**2)
