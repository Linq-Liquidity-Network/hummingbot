from guppy import hpy


class HeapCommand:
    def show_heap(self):
        h = hpy()
        self.logger().info(h.heap())
