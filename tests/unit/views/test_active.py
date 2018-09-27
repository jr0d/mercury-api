import unittest
import http

import mock

import webtest


class TestActiveEndpoints(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.patch_inventory_client = mock.patch(
            'mercury_api.mercury_clients.SimpleInventoryClient'
        )
        self.patch_inventory_client.start()

        self.mock_rpc_client = mock.patch(
            'mercury_api.mercury_clients.SimpleRPCFrontEndClient'
        )
        self.mock_rpc_client.start()

        self.mock_configuration = mock.patch('mercury_api.configuration.get_api_configuration')
        self.mock_configuration.start()

        self.mock_get_projection_from_qsa = mock.patch(
            'mercury_api.views.active.get_projection_from_qsa'
        )
        self.mock_get_projection_from_qsa.start()

        self.mock_get_paging_info_from_qsa = mock.patch(
            'mercury_api.views.active.get_paging_info_from_qsa'
        )
        self.mock_get_paging_info_from_qsa.start()

        self.mock_logging_setup = mock.patch('mercury_api.transaction_log.setup_logging')
        self.mock_logging_setup.start()

        from mercury_api import app
        self.app = webtest.TestApp(app.app)

    def tearDown(self):
        self.patch_inventory_client.stop()
        self.mock_rpc_client.stop()
        self.mock_configuration.stop()
        self.mock_get_projection_from_qsa.stop()
        self.mock_get_paging_info_from_qsa.stop()
        self.mock_logging_setup.stop()

    @mock.patch('mercury_api.views.active.inventory_client.get_one')
    def test_get_active_computer(self, mock_inv_get_one):
        self.mock_get_projection_from_qsa.return_value = None
        mercury_id = 'mrc1234'
        mock_inv_get_one.return_value = {'mercury_id': mercury_id}
        resp = self.app.get('/api/active/computers/{}'.format(mercury_id))
        self.assertEqual(resp.status_code, http.HTTPStatus.OK)
        self.assertEqual(resp.json_body, {'mercury_id': mercury_id})

    @mock.patch('mercury_api.views.active.inventory_client.get_one')
    def test_get_active_computer_not_found(self, mock_inv_get_one):
        self.mock_get_projection_from_qsa.return_value = None
        mercury_id = 'mrc1234'
        mock_inv_get_one.return_value = {}
        resp = self.app.get('/api/active/computers/{}'.format(mercury_id), expect_errors=True)
        self.assertEqual(resp.status_code, http.HTTPStatus.NOT_FOUND)
        self.assertEqual(
            resp.json_body['message'],
            'mercury_id {} does not exist in inventory'.format(mercury_id)
        )

    def test_get_active_computer_blacklist(self):
        # verifies the blacklisted values for computer are not accepted.
        resp = self.app.get('/api/active/computers/query', expect_errors=True)
        self.assertEqual(resp.status_code, http.HTTPStatus.METHOD_NOT_ALLOWED)
