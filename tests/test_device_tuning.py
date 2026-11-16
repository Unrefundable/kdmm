import tempfile
import unittest
from pathlib import Path

import kodi_stubs  # noqa: F401
import device_tuning

HELPER_SOURCE = '''    def set_pragmas(self, connection):
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection
'''


class HelperDatabaseMmapPatchTest(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.path = Path(self._dir.name) / "database.py"
        self._original = device_tuning._HELPER_DATABASE_PATH
        device_tuning._HELPER_DATABASE_PATH = str(self.path)
        self.addCleanup(setattr, device_tuning, "_HELPER_DATABASE_PATH", self._original)

    def test_adds_mmap_off_with_matching_indent_and_still_compiles(self):
        self.path.write_text(HELPER_SOURCE, encoding="utf-8")
        self.assertTrue(device_tuning.patch_helper_database_mmap())
        patched = self.path.read_text(encoding="utf-8")
        self.assertIn('        connection.execute("PRAGMA mmap_size=0")\n        return connection', patched)
        compile("class DataBase:\n" + patched, "database.py", "exec")

    def test_second_run_changes_nothing(self):
        self.path.write_text(HELPER_SOURCE, encoding="utf-8")
        device_tuning.patch_helper_database_mmap()
        once = self.path.read_text(encoding="utf-8")
        self.assertFalse(device_tuning.patch_helper_database_mmap())
        self.assertEqual(self.path.read_text(encoding="utf-8"), once)

    def test_unknown_helper_code_is_left_alone(self):
        self.path.write_text("def set_pragmas(self, connection):\n    return connection\n", encoding="utf-8")
        self.assertFalse(device_tuning.patch_helper_database_mmap())
        self.assertNotIn("mmap_size", self.path.read_text(encoding="utf-8"))

    def test_missing_helper_is_not_an_error(self):
        self.assertFalse(device_tuning.patch_helper_database_mmap())


if __name__ == "__main__":
    unittest.main()
