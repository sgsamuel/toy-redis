import math

from app.data.entries.geo import decode_coordinates, encode_coordinates, haversine_distance

from tests.support import CommandTestCase


class GeoCommandTests(CommandTestCase):
    def test_reference_scores_and_coordinate_precision(self):
        for latitude, longitude, score in [
            (13.7220, 100.5252, 3962257306574459), (39.9075, 116.3972, 4069885364908765),
            (52.5244, 13.4105, 3673983964876493), (55.6759, 12.5655, 3685973395504349),
            (28.6667, 77.2167, 3631527070936756), (27.7017, 85.3206, 3639507404773204),
            (51.5074, -0.1278, 2163557714755072), (40.7128, -74.0060, 1791873974549446),
            (48.8534, 2.3488, 3663832752681684), (-33.8688, 151.2093, 3252046221964352),
            (35.6895, 139.6917, 4171231230197045), (48.2064, 16.3707, 3673109836391743),
        ]:
            with self.subTest(latitude=latitude, longitude=longitude):
                self.assertEqual(encode_coordinates(longitude, latitude), score)
                decoded_longitude, decoded_latitude = decode_coordinates(score)
                self.assertAlmostEqual(decoded_longitude, longitude, delta=0.000003)
                self.assertAlmostEqual(decoded_latitude, latitude, delta=0.000002)

    def test_haversine_antipodes_and_date_line(self):
        radius = 6372797.560856
        self.assertEqual(haversine_distance(1, 2, 1, 2), 0)
        self.assertAlmostEqual(haversine_distance(0, 0, 180, 0), math.pi * radius)
        self.assertAlmostEqual(haversine_distance(179.9, 0, -179.9, 0), math.radians(0.2) * radius, places=6)

    async def test_inclusive_bounds_and_atomic_validation(self):
        for longitude in ['-180', '180']:
            for latitude in ['-85.05112878', '85.05112878']:
                self.assertFalse((await self.command('GEOADD', 'edge', longitude, latitude, 'member')).is_error)
        for longitude, latitude in [('181', '0'), ('0', '85.05112879'), ('nan', '0'), ('0', 'inf')]:
            self.assertTrue((await self.command('GEOADD', 'bad', '0', '0', 'valid', longitude, latitude, 'invalid')).is_error)
            self.assertIsNone(self.executor.store.type('bad'))

    async def test_positions_distances_and_missing_members(self):
        await self.command('GEOADD', 'places', '13.361389', '38.115556', 'Palermo',
                           '15.087269', '37.502669', 'Catania')
        self.assertEqual((await self.command('GEODIST', 'places', 'Palermo', 'Catania')).value, '166274.1516')
        self.assertEqual((await self.command('GEODIST', 'places', 'Palermo', 'Catania', 'km')).value, '166.2742')
        self.assertEqual((await self.command('GEODIST', 'places', 'Palermo', 'Catania', 'mi')).value, '103.3182')
        positions = (await self.command('GEOPOS', 'places', 'missing', 'Palermo')).value
        self.assertIsNone(positions[0])
        self.assertAlmostEqual(float(positions[1][0]), 13.361389, delta=0.000003)
        self.assertIsNone((await self.command('GEODIST', 'places', 'Palermo', 'missing')).value)

    async def test_search_radius_box_sort_count_and_extras(self):
        await self.command('GEOADD', 'places', '13.361389', '38.115556', 'Palermo',
                           '15.087269', '37.502669', 'Catania')
        self.assertEqual((await self.command('GEOSEARCH', 'places', 'FROMLONLAT', '15', '37',
                                            'BYRADIUS', '200', 'km', 'ASC')).value, ['Catania', 'Palermo'])
        self.assertEqual((await self.command('GEOSEARCH', 'places', 'FROMLONLAT', '15', '37',
                                            'BYBOX', '100', '200', 'km')).value, ['Catania'])
        reply = await self.command('GEOSEARCH', 'places', 'FROMMEMBER', 'Palermo', 'BYRADIUS', '200', 'km',
                                   'COUNT', '1', 'WITHCOORD', 'WITHHASH', 'WITHDIST')
        self.assertEqual(len(reply.value), 1)
        self.assertEqual(reply.value[0][:3], ['Palermo', '0.0000', 3479099956230698])
        self.assertEqual(len(reply.value[0][3]), 2)

    async def test_invalid_geo_scores_return_protocol_errors(self):
        for score in [math.inf, -math.inf, -1.0, float(2 ** 55)]:
            self.executor.store.zadd('geo', (score, 'point'))
            self.assertTrue((await self.command('GEOPOS', 'geo', 'point')).is_error)
        self.assertEqual((await self.command('PING')).value, 'PONG')

    async def test_geoadd_nx_xx_conditions(self):
        store = self.executor.store
        self.assertEqual((await self.command('GEOADD', 'places', 'xx', '0', '0', 'a')).value, 0)
        self.assertIsNone(store.type('places'))
        self.assertEqual(self.executor.replication.offset, 0)
        self.assertEqual((await self.command('GEOADD', 'places', 'nx', '0', '0', 'a')).value, 1)
        original = store.zscore('places', 'a')
        version = store.get_key_version('places')
        offset = self.executor.replication.offset
        await self.command('GEOADD', 'places', 'NX', '1', '1', 'a')
        self.assertEqual(store.get_key_version('places'), version)
        self.assertEqual(self.executor.replication.offset, offset)
        self.assertEqual((await self.command('GEOADD', 'places', 'NX', '1', '1', 'a',
                                            '2', '2', 'b')).value, 1)
        self.assertEqual(store.zscore('places', 'a'), original)
        self.assertEqual((await self.command('GEOADD', 'places', 'XX', '3', '3', 'a',
                                            '4', '4', 'missing')).value, 0)
        self.assertNotEqual(store.zscore('places', 'a'), original)
        self.assertIsNone(store.zscore('places', 'missing'))
        version = store.get_key_version('places')
        for options in [('NX', 'XX', '0', '0', 'a'), ('NX', '0', '0'),
                        ('XX', '0', '0', 'a', '181', '0', 'bad')]:
            with self.subTest(options=options):
                self.assertTrue((await self.command('GEOADD', 'places', *options)).is_error)
                self.assertEqual(store.get_key_version('places'), version)
