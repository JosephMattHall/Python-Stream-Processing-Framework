import unittest
from pspf.processing.windows import TumblingWindow, SlidingWindow

class TestWindows(unittest.TestCase):
    def test_tumbling_window(self):
        # 10s window (10000ms)
        window = TumblingWindow(size_ms=10000)
        
        # TS = 12.5s -> Window [10s, 20s)
        wins = window.assign_windows(12.5)
        self.assertEqual(len(wins), 1)
        self.assertEqual(wins[0], (10.0, 20.0))
        
        # TS = 10.0s -> Window [10s, 20s)
        wins = window.assign_windows(10.0)
        self.assertEqual(wins[0], (10.0, 20.0))
        
        # TS = 9.9s -> Window [0s, 10s)
        wins = window.assign_windows(9.9)
        self.assertEqual(wins[0], (0.0, 10.0))

    def test_sliding_window(self):
        # Size=10s, Slide=5s
        window = SlidingWindow(size_ms=10000, slide_ms=5000)
        
        # TS = 12s
        # Should belong to:
        # [5, 15)  (started at 5)
        # [10, 20) (started at 10)
        # NOT [0, 10) (ends at 10)
        # NOT [15, 25) (start at 15)
        
        wins = window.assign_windows(12.0)
        self.assertEqual(len(wins), 2)
        expected = {(5.0, 15.0), (10.0, 20.0)}
        self.assertEqual(set(wins), expected)

if __name__ == '__main__':
    unittest.main()
