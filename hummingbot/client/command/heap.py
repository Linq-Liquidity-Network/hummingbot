from datetime import datetime

from guppy import hpy

guppy_temporary_object = None
guppy_temporary_iter = None
guppy_hp = hpy()

class HeapCommand:
    def heap_update(self, refresh = False, index = None, relation = None):
        global guppy_temporary_object

        if refresh or guppy_temporary_object is None:
            guppy_temporary_object = guppy_hp.heap()

        if relation is not None:
            if relation == "bytype":
                guppy_temporary_object = guppy_temporary_object.bytype
            elif relation == "byrcs":
                guppy_temporary_object = guppy_temporary_object.byrcs
            elif relation == "byclodo":
                guppy_temporary_object = guppy_temporary_object.byclodo
            elif relation == "bysize":
                guppy_temporary_object = guppy_temporary_object.bysize
            elif relation == "byid":
                guppy_temporary_object = guppy_temporary_object.byid
            elif relation == "byvia":
                guppy_temporary_object = guppy_temporary_object.byvia
            elif relation == "referents":
                guppy_temporary_object = guppy_temporary_object.referents

        if index is not None:
            guppy_temporary_object = guppy_temporary_object[index]

        global guppy_temporary_iter
        guppy_temporary_iter = None

        self._notify(guppy_temporary_object)

    def heap_setrel(self):
        guppy_hp.setrelheap()
        self._notify(f"setrelheap called at {datetime.now().isoformat()}")

    def heap_next(self):
        global guppy_temporary_iter
        global guppy_temporary_object

        if guppy_temporary_iter is None and guppy_temporary_object is not None:
            guppy_temporary_iter = iter(guppy_temporary_object.nodes)

        if guppy_temporary_iter is not None:
            self._notify(guppy_temporary_iter.__next__())
        else:
            self._notify("heap_update must be called first")
