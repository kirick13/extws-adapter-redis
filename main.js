
const crypto = require('crypto');

const REDIS_PUBSUB_CHANNEL = 'extws';
const REGEXP_PAYLOAD_SPLIT = /^(.{8})([0-2])([^%]+)?%/;

const REDIS_TARGET_TYPES = {
	SOCKET   : '0',
	GROUP    : '1',
	BROADCAST: '2',
};

class ExtWSRedisAdapter {
	constructor (
		server,
		pub_client,
		sub_client,
	) {
		this._server = server;

		this._pub_client = pub_client;
		this._sub_client = sub_client;

		this.id = crypto.randomBytes(6).toString('base64').replace(/=/g, ''); // 8 symbols

		pub_client.on(
			'error',
			console.error,
		);

		sub_client.subscribe(
			REDIS_PUBSUB_CHANNEL,
			() => {},
		);
		sub_client.on(
			'message',
			(redis_channel, redis_message) => this._onMessage(redis_channel, redis_message),
		);
		sub_client.on(
			'error',
			console.error,
		);

		server._adapter = this;
	}

	publish (
		payload,
		socket_id = null,
		group_id = null,
		is_broadcast = false,
	) {
		let redis_payload = this.id;

		if (typeof socket_id === 'string') {
			if (this._server.clients.has(socket_id)) {
				return;
			}

			redis_payload += REDIS_TARGET_TYPES.SOCKET + socket_id;
		}
		if (typeof group_id === 'string') {
			redis_payload += REDIS_TARGET_TYPES.GROUP + group_id;
		}
		else if (is_broadcast) {
			redis_payload += REDIS_TARGET_TYPES.BROADCAST;
		}

		redis_payload += '%' + payload;

		this._pub_client.publish(
			REDIS_PUBSUB_CHANNEL,
			redis_payload,
		);
	}

	_onMessage (redis_channel, redis_message) {
		if (redis_channel.toString() === REDIS_PUBSUB_CHANNEL) {
			redis_message = redis_message.toString();

			const [ matched, adapter_id, type, dest_id ] = redis_message.match(REGEXP_PAYLOAD_SPLIT);

			if (adapter_id !== this.id) {
				const payload = redis_message.slice(matched.length);

				let socket_id = null;
				let group_id = null;
				let is_broadcast = false;

				switch (type) {
					case REDIS_TARGET_TYPES.SOCKET:
						socket_id = dest_id;
					break;
					case REDIS_TARGET_TYPES.GROUP:
						group_id = dest_id;
					break;
					case REDIS_TARGET_TYPES.BROADCAST:
						is_broadcast = true;
					break;
					// no default
				}

				this._server._sendPayload(
					payload,
					socket_id,
					group_id,
					is_broadcast,
					true, // is_from_adapter
				);
			}
		}
	}
}

module.exports = ExtWSRedisAdapter;
