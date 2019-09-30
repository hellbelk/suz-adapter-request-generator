const uuid = require('uuid').v4;
const kafka = require('kafka-node'),
    client = new kafka.KafkaClient(),
    producer = new kafka.Producer(client);

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

if (!params.requestType) {
    client.close();
    console.log("requestType is required")
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

const createRequest = type => {
    let req = null
    switch (type) {
        case 'CREATE_ORDER_FOR_EMISSION_IC': req = emissionRequest(); break;
        case 'CHANGE_STATUS': req = changeStatusRequest(); break;
    }

    console.log(JSON.stringify(req, null , 2));

    return JSON.stringify(req);
};

producer.on('ready', () => {
    producer.send([{topic: 'adaptersuz.input', messages: createRequest(params.requestType)}], (error, data) => {
        if (error) {
            console.error('Error!!!');
            console.error(error);
        } else {
            console.log('Success');
        }
        client.exit();
    })
});

producer.on('error', function (err) {
    console.error(err);
    client.close();
});