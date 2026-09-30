"""Page cache. The only component that reads or writes a data file."""
import os

PAGE_SIZE = 8192
MAX_PAGES = 5000


def fsync_dir(path):
    """Make a rename durable: the directory entry has to reach disk too."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Pager:
    def __init__(self, filename):
        self.filename = filename
        if not os.path.exists(filename):
            open(filename, "wb").close()
        self.file = open(filename, "rb")
        self.file_length = os.path.getsize(filename)
        self.num_pages = -(-self.file_length // PAGE_SIZE)
        self.pages = [None] * MAX_PAGES

    def get_page(self, page_num):
        if page_num >= MAX_PAGES:
            raise RuntimeError(f"{self.filename} is full ({MAX_PAGES} pages)")
        if self.pages[page_num] is None:
            if page_num < self.num_pages:
                self.file.seek(page_num * PAGE_SIZE)
                data = self.file.read(PAGE_SIZE)
                self.pages[page_num] = bytearray(data.ljust(PAGE_SIZE, b"\x00"))
            else:
                self.pages[page_num] = bytearray(PAGE_SIZE)
                self.num_pages = page_num + 1
        return self.pages[page_num]

    def checkpoint(self):
        """Persist every page by writing a new file and renaming it over the old one.

        The file on disk is therefore always a complete, consistent tree: either
        the previous checkpoint or this one, never a half-written mix of both.
        """
        # ponytail: rewrites the whole file, O(database size). Track dirty pages
        # and add a double-write buffer once files outgrow memory.
        tmp = self.filename + ".tmp"
        with open(tmp, "wb") as out:
            for page_num in range(self.num_pages):
                out.write(self.get_page(page_num))
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, self.filename)
        fsync_dir(os.path.dirname(os.path.abspath(self.filename)))
        self.file.close()
        self.file = open(self.filename, "rb")

    def close(self):
        self.file.close()
