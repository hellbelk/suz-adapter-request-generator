const {Client} = require('pg');

const client = new Client();

const messageStackByRequestId = async requestId => {
    const res = await client.query(`
        select ms.gtin, ms.lastblockid, mio.identification_value as orderId from messages.adapter_message_identification mi
        join messages.adapter_message_stack ms on mi.adapter_message_id = ms.adapter_message_id
        join messages.adapter_message_identification mio on mi.adapter_message_id = mio.adapter_message_id
        where mi.system_id = 'CTT' and mi.identification_type = 'GUID' and mi.identification_value = $1
        and mio.system_id = 'OMS' and mio.identification_type = 'orderId'`, [requestId]);
    return res.rows[0];
};

const getCodes = async requestId => {
    const res = await client.query(`
        select c.cryptocode as code from messages.cryptocodes c
        where exists (
            select * from messages.adapter_message_stack ms
            where c.adapter_message_stack_id = ms.adapter_message_stack_id
            and exists (
                select * from messages.adapter_message_identification mi
                where mi.adapter_message_id = ms.adapter_message_id
                and mi.system_id = 'CTT' and mi.identification_type = 'GUID' and mi.identification_value = $1
            )
        )`, [requestId]);
    return res.rows.map(r => r.code);
};

function wrap(fn) {
    return async function () {
        let res;
        const args = [...arguments];
        client.connect();
        try {
            res = await fn.apply(this, args);
        } catch (e) {}
        client.end();

        return res;
    }
}

module.exports = {
    messageStackByRequestId: wrap(messageStackByRequestId),
    getCodes: wrap(getCodes)
};