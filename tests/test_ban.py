import unittest
from itertools import pairwise
from operator import itemgetter
from unittest.mock import patch

from fgpbot.ban import (
    HEATED,
    MAX_ENTRIES,
    RANDOM,
    SCENE_TTL,
    SEQUENCES,
    SPECIAL,
    BanResponder,
)


class BanResponderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.responder = BanResponder()
        choice = patch("fgpbot.ban.secrets.choice", side_effect=itemgetter(0))
        choice.start()
        self.addCleanup(choice.stop)

    def test_scene_is_shared_by_target_and_random_replies_do_not_repeat(self) -> None:
        replies = [
            self.responder.reply("target", str(i), kind="viewer", now=float(i)) for i in range(7)
        ]
        self.assertEqual(replies[:3], list(SEQUENCES[0]))
        for previous, reply in pairwise(replies[2:]):
            self.assertIn(reply, RANDOM)
            self.assertNotEqual(previous, reply)
        self.assertEqual(
            self.responder.reply("other", "invoker", kind="viewer", now=8), SEQUENCES[0][0]
        )

    def test_other_scene_can_be_selected_and_keeps_its_order(self) -> None:
        with patch("fgpbot.ban.secrets.choice", return_value=SEQUENCES[1]):
            first = self.responder.reply("target", "invoker", kind="viewer", now=0)
        self.assertEqual(first, SEQUENCES[1][0])
        for i in (1, 2):
            self.assertEqual(
                self.responder.reply("target", "invoker", kind="viewer", now=i),
                SEQUENCES[1][i],
            )

    def test_expiry_is_measured_from_last_scene_reply(self) -> None:
        self.responder.reply("target", "invoker", kind="viewer", now=0)
        self.assertEqual(
            self.responder.reply("target", "invoker", kind="viewer", now=SCENE_TTL - 1),
            SEQUENCES[0][1],
        )
        self.assertEqual(
            self.responder.reply("target", "invoker", kind="viewer", now=2 * SCENE_TTL - 1),
            SEQUENCES[0][0],
        )

    def test_heat_follows_invoker_without_consuming_target_scene(self) -> None:
        for i in range(3):
            self.responder.reply(str(i), "spammer", kind="viewer", now=float(i))
        self.assertIn(self.responder.reply("new", "spammer", kind="viewer", now=3), HEATED)
        self.assertEqual(
            self.responder.reply("new", "someone_else", kind="viewer", now=3), SEQUENCES[0][0]
        )
        self.assertEqual(
            self.responder.reply("0", "spammer", kind="viewer", now=4), SEQUENCES[0][1]
        )

    def test_heat_window_boundary_and_repeated_bursts(self) -> None:
        for i in range(3):
            self.responder.reply(str(i), "spammer", kind="viewer", now=float(i))
        self.assertEqual(
            self.responder.reply("3", "spammer", kind="viewer", now=10), SEQUENCES[0][0]
        )
        first = self.responder.reply("4", "spammer", kind="viewer", now=10.5)
        self.assertIn(first, HEATED)
        for i in range(3):
            self.assertNotIn(
                self.responder.reply(str(i), "spammer", kind="viewer", now=11 + i), HEATED
            )
        second = self.responder.reply("4", "spammer", kind="viewer", now=14)
        self.assertIn(second, HEATED)
        self.assertNotEqual(first, second)

    def test_special_replies_do_not_repeat_or_consume_viewer_scene(self) -> None:
        for kind, pool in SPECIAL.items():
            with self.subTest(kind=kind):
                previous = ""
                for i in range(6):
                    reply = self.responder.reply("target", str(i), kind=kind, now=20 * i)
                    self.assertIn(reply, pool)
                    self.assertNotEqual(reply, previous)
                    previous = reply
        self.assertEqual(
            self.responder.reply("target", "new", kind="viewer", now=120), SEQUENCES[0][0]
        )

    def test_dialogue_memory_is_bounded(self) -> None:
        for i in range(MAX_ENTRIES + 1):
            self.responder.reply(str(i), str(i), kind="viewer", now=0)
        self.assertEqual(len(self.responder._scenes), MAX_ENTRIES)
        self.assertEqual(len(self.responder._bursts), MAX_ENTRIES)
        self.assertNotIn(("viewer", "0"), self.responder._scenes)
        self.assertNotIn("0", self.responder._bursts)

    def test_all_templates_render_with_only_supported_mentions(self) -> None:
        pools = (*SEQUENCES, RANDOM, HEATED, *SPECIAL.values())
        for pool in pools:
            for template in pool:
                with self.subTest(template=template):
                    text = template.format(target="@target", invoker="@invoker")
                    self.assertNotIn("{", text)
                    self.assertNotIn("@@", text)
                    self.assertLessEqual(len(text), 200)
