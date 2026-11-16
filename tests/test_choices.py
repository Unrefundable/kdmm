import re
import tempfile
import unittest
from pathlib import Path

import kodi_stubs
import choices

SETTINGS_XML = Path(__file__).resolve().parents[1] / "resources" / "settings.xml"


class ChoiceIndexTest(unittest.TestCase):
    def setUp(self):
        kodi_stubs.reset()

    def test_old_index_values_still_read(self):
        self.assertEqual(choices.index_of("hdr_priority", "2"), 2)

    def test_label_values_read_as_their_index(self):
        self.assertEqual(choices.index_of("hdr_priority", "HDR10"), 2)
        self.assertEqual(choices.index_of("hdr_priority", "sdr only"), 4)

    def test_empty_unknown_and_out_of_range_fall_back(self):
        self.assertEqual(choices.index_of("source_priority", ""), 0)
        self.assertEqual(choices.index_of("source_priority", "Betamax", default=3), 3)
        self.assertEqual(choices.index_of("source_priority", "99"), 0)

    def test_choice_index_reads_the_addon_setting(self):
        kodi_stubs.FakeAddon.settings["debrid_provider_order"] = "AllDebrid first"
        self.assertEqual(choices.choice_index("debrid_provider_order"), 1)

    def _saved(self, values):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        path = Path(folder.name) / "settings.xml"
        rows = "".join(f'<setting id="{k}">{v}</setting>' for k, v in values.items())
        path.write_text(f'<settings version="2">{rows}</settings>', encoding="utf-8")
        return str(path)

    def test_migration_reads_indexes_from_the_saved_file(self):
        # Kodi itself reports the default for a stored index it cannot map.
        kodi_stubs.FakeAddon.settings["preferred_audio_language"] = "Any"
        path = self._saved({
            "resolution_priority": "1",
            "tv_pack_preference": "Balanced",
            "preferred_audio_language": "2",
            "rd_api_key": "5",
        })
        self.assertEqual(choices.migrate_choice_settings(settings_path=path), 2)
        saved = kodi_stubs.FakeAddon.settings
        self.assertEqual(saved["resolution_priority"], "1080p")
        self.assertEqual(saved["preferred_audio_language"], "Japanese")
        self.assertNotIn("tv_pack_preference", saved)
        self.assertNotIn("rd_api_key", saved)

    def test_migration_without_a_saved_file_changes_nothing(self):
        self.assertEqual(choices.migrate_choice_settings(settings_path="/nonexistent/settings.xml"), 0)
        self.assertEqual(kodi_stubs.FakeAddon.settings, {})


class SettingsXmlTest(unittest.TestCase):
    def test_every_choice_is_a_select_matching_the_code(self):
        text = SETTINGS_XML.read_text(encoding="utf-8")
        self.assertNotIn('type="enum"', text)
        for key, options in choices.CHOICES.items():
            match = re.search(rf'<setting id="{key}"[^>]*/>', text)
            self.assertIsNotNone(match, key)
            tag = match.group(0)
            self.assertIn('type="select"', tag)
            values = re.search(r'values="([^"]*)"', tag).group(1).split("|")
            self.assertEqual(tuple(values), options, key)
            default = re.search(r'default="([^"]*)"', tag).group(1)
            self.assertIn(default, options, key)


if __name__ == "__main__":
    unittest.main()
