require('dotenv').config();
const http = require('http');
const host = process.env.KAFKA_HOST;
const uuid = require('uuid').v4;
const kafka = require('kafka-node');
const db = require('./db');

const numberTypes = ['quantity', 'time'];

const params = process.argv.slice(2).reduce((obj, raw) => {
    if (raw.indexOf('--') === 0) {
        const eqInx = raw.indexOf('=');
        if (eqInx !== -1) {
            const key = raw.substring(2, eqInx);
            const valueString = raw.substring(eqInx + 1);
            obj[key] = numberTypes.indexOf(key) !== -1 ? +valueString : valueString;
        } else {
            obj[raw.substring(2)] = true;
        }
    }
    return obj;
}, {});

if (!params.requestType && !params.help) {
    console.log("requestType is required");
    return;
}

if (!host) {
    console.log("KAFKA_HOST environment variable is required");
    return;
}

if (params.help) {
    if (!params.requestType) {
        console.log(`available request types:
PING
CREATE_ORDER_FOR_EMISSION_IC
GET_IC_BUFFER_STATUS
GET_ICS_FROM_THE_ORDER
CHANGE_STATUS
ROUTING`)
    } else {
        switch (params.requestType) {
            case "CREATE_ORDER_FOR_EMISSION_IC":
                console.log(`Create request for emission
`);
                break;
        }
    }
    return;
}

if (!params.cttId) {
    params.cttId = '77f5ccc7bd27d7'
}

const GTIN_SYMBOLS = (() => {
    let r = '';
    for (let i = 0; i < 10; i++) {
        r += i;
    }
    return r;
})();

const SN_SYMBOLS = (() => {
    const charCode = char => char.charCodeAt(0);
    let r = '';
    for (let i = charCode('a'); i <= charCode('z'); i++) {
        r += String.fromCharCode(i);
    }

    for (let i = charCode('A'); i <= charCode('Z'); i++) {
        r += String.fromCharCode(i);
    }

    for (let i = 0; i < 10; i++) {
        r += i;
    }

    return r;
})();

const random = (symbols, len) => {
    let result = '';
    for (let i = 0; i < len; i++) {
        const inx = Math.round((Math.random() * 1000) % (symbols.length -1));
        result += symbols[inx];
    }
    return result;
};

const generateGTIN = () =>
    random(GTIN_SYMBOLS, 14);

const generateSN = () =>
    random(SN_SYMBOLS, 13);

function emissionRequest() {
    let quantity = params.quantity ? params.quantity : Math.round((Math.random() * 100) % 50);
    if (!quantity) {
        quantity = 1;
    }

    const serialNumbers = [];
    for (let i = 0; i < quantity; i++) {
        serialNumbers.push(generateSN());
    }


    const order = {
        products: [
            {
                gtin: generateGTIN(),
                quantity: quantity,
                serialNumberType: 'SELF_MADE',
                serialNumbers: serialNumbers,
                templateId: 2,
            }
        ],
        subjectId: uuid(),
        freeCode: true,
        paymentType: 2
    };

    const request = {
        requestId: uuid(),
        requestType: 'CREATE_ORDER_FOR_EMISSION_IC',
        params: {
            cttId: params.cttId,
            request: JSON.stringify(order)
        }
    };

    return request;
}

function pingRequest() {
    const request = {
        requestId: uuid(),
        requestType: 'PING',
        params: {
            cttId: params.cttId
        }
    };

    return request;
}

async function getBufferStatus() {
    const request = {
        requestId: uuid(),
        requestType: 'GET_IC_BUFFER_STATUS',
        params: {
            cttId: params.cttId,
            requestId: params.requestId, //75ed6b41-3750-4d76-9210-89137f8bd053
            gtin: params.gtin
        }
    };

    if (!params.requestId) {
        console.log('requestId param is required');
    } else {
        request.params.gtin = (await db.messageStackByRequestId(params.requestId)).gtin
    }

    return request;
}

async function getICsFromOrder() {
    const request = {
        requestId: uuid(),
        requestType: 'GET_ICS_FROM_THE_ORDER',
        params: {
            cttId: params.cttId,
            requestId: params.requestId, //75ed6b41-3750-4d76-9210-89137f8bd053
        }
    };

    if (!params.requestId) {
        console.log('requestId param is required');
    } else {
        request.params.gtin = (await db.messageStackByRequestId(params.requestId)).gtin
    }

    return request;
}

function changeStatusRequest() {
    const request = {
        requestId: uuid(),
        requestType: 'CHANGE_STATUS',
        params: {
            cttId: params.cttId,
            requestId: params.requestId,
        }
    };

    if (params.status) {
        request.params.status = params.status;
    }

    if (params.time) {
        request.params.time = params.time;
    }

    return request;
}

async function routingRequest() {
    if (!params.type) {
        console.log('routing type is required');

    }

    const request = {
        requestId: uuid(),
        requestType: 'ROUTING',
        params: {
            cttId: params.cttId,
            type: params.type
        }
    };

    switch(params.type) {
        case 'close_suborder':
            if (params.requestId) {
                const r = await db.messageStackByRequestId(params.requestId);
                request.params.lastBlockId = r.lastblockid;
                request.params.gtin = r.gtin;
                request.params.orderId = r.orderid;
            } else {
                console.log('requestId is required')
            }
            break;
        case 'usage_report':
            const body = {
                usageType: 'VERIFIED',
                expirationDate: `01.01.${new Date().getFullYear()}`,
                orderType: 1,
                seriesNumber: '123',
                subjectId: '00000000000397'
            };
            if (params.requestId) {
                body.sntins = await db.getCodes(params.requestId);
            } else {
                console.log('requestId is required')
            }
            request.params.request = JSON.stringify(body);
            break;
        case 'get_reports_status':
            if (params.reportId) {
                request.params.reportId = params.reportId
            } else {
                console.log('reportId is required')
            }
            break;
    }
    return request
}

const testdata = {
  "requestId": uuid(),
  "requestType": "CREATE_ORDER_FOR_EMISSION_IC",
  "params": {
    "cttId": "77f5ccc7bd27d7",
    "request": "{\"products\":[{\"gtin\":\"82073459958601\",\"quantity\":7,\"serialNumberType\":\"SELF_MADE\",\"serialNumbers\":[\"sUOeKviSNXTog\",\"cCQnFBssWAqCi\",\"LdJ1Biw3VWhMG\",\"0MhRBCsjjpLTG\",\"jPwRbmwwKdhi5\",\"lTmgjkWPDkLIk\",\"0mG6mwZOfw2TQ\"],\"templateId\":2}],\"subjectId\":\"47a2a63d-f814-46a5-b3b4-f270f6a2051a\",\"freeCode\":true,\"paymentType\":2}"
  }
}

const  ttt = {"poolInfos":[{"status":"REJECTED","quantity":7,"leftInRegistrar":7,"rejectionReason":"Order #261944 backup failed: An attempt to add existing marking code. Key (cis)=(018207345995860121sUOeKviSNXTog) already exists.","registrarId":"0000000077770003","isRegistrarReady":true,"registrarErrorCount":0,"lastRegistrarErrorTimestamp":0}],"leftInBuffer":0,"poolsExhausted":true,"totalCodes":7,"unavailableCodes":7,"availableCodes":0,"orderId":"f97a06c7-118f-44a8-a6e5-65f13c7b08df","gtin":"82073459958601","bufferStatus":"REJECTED","rejectionReason":"All pools in statuses: REJECTED or REQUEST_ERROR","totalPassed":0,"omsId":"2d08c2fa-f9af-47ef-8ceb-295768494c5b"}

const createRequest = async type => {
    let req = null;
    switch (type) {
        case 'PING': req = pingRequest(); break;
        // case 'CREATE_ORDER_FOR_EMISSION_IC': req = testdata; break;
        case 'CREATE_ORDER_FOR_EMISSION_IC': req = emissionRequest(); break;
        case 'GET_IC_BUFFER_STATUS': req = await getBufferStatus(); break;
        case 'GET_ICS_FROM_THE_ORDER': req = await getICsFromOrder(); break;
        case 'CHANGE_STATUS': req = changeStatusRequest(); break;
        case 'ROUTING': req = await routingRequest(); break;
    }

    console.log(JSON.stringify(req, null , 2));

    return JSON.stringify(req);
};
// (async () => {
//     await createRequest(params.requestType);
// })();
//
// return;

console.log(`connecting to ${host}`);
const client = new kafka.KafkaClient(host),
producer = new kafka.Producer(client);

producer.on('ready', async () => {
    producer.send([{topic: 'adaptersuz.input', messages: await createRequest(params.requestType)}], (error, data) => {
        if (error) {
            console.error('Error!!!');
            console.error(error);
        } else {
            console.log('Success');
        }
        producer.close();
        client.close();
        process.exit(0);
    })
});

producer.on('error', function (err) {
    console.error(err);
    producer.close();
    client.close();
    process.exit(1);
});

process.on('beforeExit', () => {
    client.close();
});

