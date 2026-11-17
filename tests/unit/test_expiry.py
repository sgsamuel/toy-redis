import time
import unittest
from unittest.mock import patch

from app.commands.common import ClientSession
from tests.support import make_executor


class ExpiryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.executor = make_executor()
        self.session = ClientSession()

    async def command(self, *tokens):
        return await self.executor.execute(list(tokens), self.session)

    async def test_conditional_set(self):
        self.assertIsNone((await self.command("SET", "k", "v", "XX")).value)
        await self.command("SET", "k", "v", "nx", "PX", "60000")
        store = self.executor.store
        version = store.get_key_version("k")
        offset = self.executor.replication.offset
        self.assertIsNone((await self.command("SET", "k", "other", "NX")).value)
        self.assertEqual(store.get_key_version("k"), version)
        self.assertEqual(self.executor.replication.offset, offset)
        self.assertEqual(store.get("k"), "v")
        await self.command("SET", "k", "other", "XX")
        self.assertEqual((await self.command("TTL", "k")).value, -1)
        for options in [("NX", "XX"), ("PX", "1", "EX", "2"), ("PX",)]:
            self.assertTrue((await self.command("SET", "k", "bad", *options)).is_error)
        await self.command("PEXPIRE", "k", "0")
        self.assertEqual((await self.command("SET", "k", "new", "NX")).value, "OK")

    async def test_decrement_and_expiry_preserved(self):
        self.assertEqual((await self.command("DECR", "new")).value, -1)
        await self.command("SET", "k", "0", "PX", "60000")
        deadline = self.executor.store._store["k"].expires_at
        self.assertEqual((await self.command("DECR", "k")).value, -1)
        self.assertEqual(self.executor.store._store["k"].expires_at, deadline)
        for value in [str(-(2 ** 63)), str(2 ** 63), "bad"]:
            await self.command("SET", "k", value)
            self.assertTrue((await self.command("DECR", "k")).is_error)
            self.assertEqual(self.executor.store.get("k"), value)

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
