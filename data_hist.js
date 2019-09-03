require("dotenv").config();
const isoDate = require("isodate");
const MongoClient = require("mongodb").MongoClient;
const fs = require("fs");
const _progress = require("cli-progress");
const CUT_MONTHS = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"];

const MONTHS = ["ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO", "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"]

async function connection() {
    return new Promise((resolve, reject) => {
        const url = process.env.PRD_MONGO_CONECTION_STRING;
        MongoClient.connect(url, { useNewUrlParser: true }, async function (
            err,
            db
        ) {
            if (err) reject(err);
            resolve(db.db(process.env.MONGO_DATABASE));
        });
    });
}

async function interval(
    collection,
    fechaInicio,
    fechaFin,
    INTERVAL,
    monthYear
) {
    console.log("cargando intervalo", fechaInicio, fechaFin);
    var cursor = await collection
        .find({
            $and: [
                {
                    monthYear: monthYear
                },
                {
                    type: 'process'
                },
                {
                    createAt: {
                        $gte: fechaInicio,
                        $lt: fechaFin
                    }
                },
            ]
        })
        .limit(INTERVAL)
        .toArray();

    let EOF = cursor && cursor.length == INTERVAL ? false : true;
    let fechaUltimo = !EOF
        ? new Date(cursor[cursor.length - 1].createAt).getTime()
        : fechaFin + 1;

    return { body: cursor, EOF: EOF, fechaUltimo: fechaUltimo };
}

const createDir = (path) => {
    //check if dir exist
    fs.stat(path, (err, stats) => {
        if (stats && stats.isDirectory()) {
            //do nothing
        } else {
            //if the given path is not a directory, create a directory
            fs.mkdirSync(path);
        }
    });
};
async function cargarDataLogs(dbo, fecha, month) {
    console.log(`Cargando data dia= ${fecha.d}`)
    fecha.d = `${fecha.d}`.length == 1 ? `0${fecha.d}` : `${fecha.d}`;
    fecha.m = `${fecha.m}`.length == 1 ? `0${fecha.m}` : `${fecha.m}`;
    let monthYear = month + fecha.a;
    let fechaInicio = new Date(
        `${fecha.a}-${fecha.m}-${fecha.d}T00:00:00.000Z`
    ).getTime();
    let fechaFin = new Date(
        `${fecha.a}-${fecha.m}-${fecha.d}T23:59:59.999Z`
    ).getTime();


    let result = { body: null, EOF: false, fechaUltimo: fechaInicio };
    const collection = dbo.collection("logs_new");
    const INTERVAL = 1000;
    let arrayResult = [];
    do {
        result = await interval(collection, fechaInicio, fechaFin, INTERVAL, monthYear
        );
        if (result.body) {
            arrayResult.push(...result.body);
        }
        fechaInicio = result.fechaUltimo;
    } while (!result.EOF);

    console.log("fin de carga de data ....");
    let nameFile = `data_${fecha.d}_${fecha.m}_${fecha.a}.txt`;
    console.log("almacenando archivo", nameFile, "total documentos=", arrayResult.length);

    fs.writeFile(`./data_out/data_${month}/${nameFile}`, JSON.stringify(arrayResult), function (
        err
    ) {
        if (err) {
            return console.log(err);
        }

        console.log("The file was saved!");
    });
}

async function principal(dbo, day, month) {
    return new Promise(async (resolve, reject) => {

        let fecha = { d: day, m: month, a: 2019 };
        createDir(`./data_out/data_${CUT_MONTHS[month - 1]}`)
        await cargarDataLogs(dbo, fecha, CUT_MONTHS[month - 1]);
        resolve();
    });
}

function loadDataFile(fileName) {
    return new Promise((resolve, reject) => {
        fs.readFile(`${fileName}`, "utf-8", (err, data) => {
            if (err) {
                if (err.code == 'ENOENT') {
                    resolve("[]")
                    return
                } else {
                    console.log(err);
                    resolve({ errorCode: 500, messageError: "Falla en carga de data" });
                    return;
                }

            }

            resolve(data);
        });
    });
}
function saveDataFile(data) {
    fs.writeFile("./dataResult.txt", data, function (err) {
        if (err) {
            return console.log(err);
        }

        console.log("The file was saved!");
    });
}

function saveDataFile(path, nameFile, data) {
    //crear directorio
    return new Promise((resolve, reject) => {
        createDir(path)
        console.log(`${path}/${nameFile}`)

        fs.appendFile(`${path}/${nameFile}`, data + "\n", 'utf8', function (
            err
        ) {
            if (err) {
                console.error('<<<<< Error   >>>>>   almacenando data en archivo', err);
                resolve(-1)
                return
            }

            console.log('successful save data')
            resolve(1)
        });
    })

}

async function loadDataMessages(day, month) {
    return new Promise(async (resolve, reject) => {
        let _day = `${day}`.length == 1 ? `0${day}` : `${day}`;
        let _month = `${month}`.length == 1 ? `0${month}` : `${month}`;
        let nameFileOut = `./data_out/data_${CUT_MONTHS[month - 1]}/data_${_day}_${_month}_2019.txt`

        let fecha = `${day}/${month}/2019`
        console.log(`fecha =${fecha}   ${nameFileOut}`)
        let result = await loadDataFile(nameFileOut);
        let messagesOut = await JSON.parse(`{"messages":${result}}`);
        if (result.errorCode) {
            console.log(result);
            return;
        }

        console.log(`analisis fecha= ${_day}_${_month}_2019`);
    
        let pedroConversations = {
            conversations: []
        }
        const bar1 = new _progress.Bar();
        bar1.start(messagesOut.messages.length, 0);
        const facetas = ['CAROUSEL_FILTER_01',
            'CAROUSEL_FILTER_02',
            'CAROUSEL_FILTER_03',
            'CAROUSEL_FILTER_04',
            'CAROUSEL_FILTER_05',
            'CAROUSEL_FILTER_06',
            'CAROUSEL_FEEDBACK_UP',
            'CAROUSEL_FEEDBACK_DOWN']

        let filters = {
            'CAROUSEL_FILTER_01': 0,
            'CAROUSEL_FILTER_02': 0,
            'CAROUSEL_FILTER_03': 0,
            'CAROUSEL_FILTER_04': 0,
            'CAROUSEL_FILTER_05': 0,
            'CAROUSEL_FILTER_06': 0,
            'CAROUSEL_FEEDBACK_UP': 0,
            'CAROUSEL_FEEDBACK_DOWN': 0,
            'Total': 0
        }

        messagesOut.messages.map((message, i) => {
            bar1.update(i)
            messageText=message.text
            if (facetas.includes(messageText)) {
                filters[messageText] = filters[messageText] + 1
                filters['Total'] = filters['Total'] + 1

            }
        })

        bar1.stop()

        let data = { fecha, filters }
        let strData = `,${JSON.stringify(data)}`

        await saveDataFile(`./result_data`, `result_filter_${_month}_2019.dat`, strData)

        resolve(`analisis fecha= ${_day}_${_month}_2019`);
    });
}

async function analisisData( month, diasMes) {

    for (let j = 1; j <= diasMes; j++) {
        await loadDataMessages( j, month)
    }
    console.log('\n\n<<<<<<< Finalizado....')
}

async function cargarData(month) {
    const dbo = await connection();
    createDir('./result_data');
    createDir('./data_out');

    console.log(`cargando data mes: ${MONTHS[month - 1]}`);
    let diasMes = new Date(2019, month, 0).getDate();
    for (let i = 1; i <= diasMes; i++) {
        await principal(dbo, i, month);
    }

    await analisisData( month, diasMes);
}

/**
 * CAMBIAR EL VALOR DEL MES Y CARGARA LA DATA DE ESE MES
 */
const MONTH = 9

cargarData(MONTH);
