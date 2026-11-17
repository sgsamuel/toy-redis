from unittest.mock import patch
import time

from tests.support import CommandTestCase


class ExpiryCommandTests(CommandTestCase):
    async def test_ttl_family(self):
        for command in ["TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME"]:
            self.assertEqual((await self.command(command, "missing")).value, -2)
        self.executor.store.rpush("list", "a")
        self.assertEqual((await self.command("TTL", "list")).value, -1)
        with patch("time.time", return_value=1000), patch("time.monotonic", return_value=time.monotonic()):
            for command, duration in [("EXPIRE", "60"), ("PEXPIRE", "60000"),
                                      ("EXPIREAT", "1060"), ("PEXPIREAT", "1060000")]:
                self.assertEqual((await self.command(command, "list", duration)).value, 1)
                for read, expected in [("TTL", 60), ("PTTL", 60000),
                                       ("EXPIRETIME", 1060), ("PEXPIRETIME", 1060000)]:
                    self.assertEqual((await self.command(read, "list")).value, expected)
            self.assertEqual((await self.command("EXPIRE", "list", "90", "NX")).value, 0)
            self.assertEqual((await self.command("EXPIRE", "list", "30", "GT")).value, 0)
            self.assertEqual((await self.command("EXPIRE", "list", "30", "LT")).value, 1)
            self.assertEqual((await self.command("PERSIST", "list")).value, 1)
            self.assertEqual((await self.command("PERSIST", "list")).value, 0)
            self.assertEqual((await self.command("EXPIRE", "list", "1", "XX")).value, 0)
            self.assertEqual((await self.command("EXPIRE", "list", "0")).value, 1)
            self.assertEqual((await self.command("TTL", "list")).value, -2)
        for tokens in [("EXPIRE", "k", "bad"), ("TTL",), ("DECR",),
                       ("EXPIRE", "k", "1", "bad")]:
            self.assertTrue((await self.command(*tokens)).is_error)
