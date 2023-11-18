import {
    Collection,
    Db,
    Document,
    FindOptions,
    InsertManyResult,
    MongoClient,
    WithId,
} from "mongodb";
import express, { Request, Response } from "express";

import bodyParser from "body-parser";
import logger from "morgan";

const app = express();

const serverPort = process.env.SERVER_PORT || 3000;
const mongoDatabase = process.env.MONGO_DB || "logs-manager";
const mongoCollection = process.env.MONGO_COLLECTION || "logs";
const mongoUser = process.env.MONGO_USER;
const mongoPassword = process.env.MONGO_PASSWORD;
const mongoHost = process.env.MONGO_HOST;
const serviceName = process.env.SERVICE_NAME;

const client: MongoClient = buildMongoClient();

function buildMongoClient(): MongoClient {
    const url = process.env.DEV
        ? "mongodb://localhost:27017"
        : `mongodb+srv://${mongoUser}:${mongoPassword}@${mongoHost}?retryWrites=true&w=majority`;

    return new MongoClient(url);
}

/* 
   example of how a caching layer could work for documentdb for connection management with serverless
   (kept in one file for ease of viewing in blog post and as this is simply a poc) 
*/
async function main(): Promise<void> {
    await databaseConnect();
    startServer();
}

// connects to the documentdb database
async function databaseConnect(): Promise<void> {
    try {
        await client.connect();
        console.log("connected successfully to server");

        const db: Db = client.db(mongoDatabase);
        console.log(`connected successfully to database ${mongoDatabase}`);

        app.locals.db = db;
    } catch (error) {
        console.log(`Error worth logging: ${error}`);
        throw new Error("unable to connect");
    }
}

// starts the server passing in the db context which is connected
function startServer() {
    try {
        app.use(logger("tiny"));
        app.use(bodyParser.json());

        app.listen(serverPort, () =>
            console.log(`${serviceName} listening on port ${serverPort}`)
        );

        // insertOne - https://docs.mongodb.com/manual/reference/method/db.collection.insertOne/
        app.post(
            "/insert",
            async (
                req: Request,
                res: Response
            ): Promise<express.Response<any, Record<string, any>>> => {
                try {
                    let { body } =
                        req.body;

                    const collection: Collection<any> =
                        app.locals.db.collection(mongoCollection);

                    // if body is not an array then wrap in an array
                    if (!Array.isArray(body)) {
                        body = [body];
                    }
                    const insertResult: InsertManyResult = await collection.insertMany(
                        body
                    );

                    console.log(`insertMany - body: ${JSON.stringify(body)}`);

                    // log the current amount of connections
                    const { connections } = await app.locals.db.admin().serverStatus();
                    console.log(
                        `insertMany - current connections: ${connections.current}, available: ${connections.available}`
                    );

                    return res.send(insertResult);
                } catch (error) {
                    console.log(error);
                    return res.sendStatus(500);
                }
            }
        );

        // find based on query filter - https://docs.mongodb.com/manual/reference/method/db.collection.find/
        app.post(
            "/find",
            async (
                req: Request,
                res: Response
            ): Promise<express.Response<any, Record<string, any>>> => {
                try {
                    const {
                        queryFilter,
                        projection,
                        limit,
                        sort,
                        skip,
                        hint,
                        min,
                        max,
                        // and the other options besides
                    } = req.body;

                    const options: FindOptions<Document> = {
                        projection,
                        limit,
                        sort,
                        skip,
                        hint,
                        min,
                        max,
                    };

                    const collection: Collection<any> =
                        app.locals.db.collection(mongoCollection);

                    const filteredDocs: WithId<any>[] = await collection
                        .find(queryFilter, options)
                        .toArray();

                    console.log(`find - body: ${JSON.stringify(queryFilter)}`);

                    // log the current amount of connections
                    const { connections } = await app.locals.db.admin().serverStatus();
                    console.log(
                        `find - current connections: ${connections.current}, available: ${connections.available}`
                    );

                    return res.send(filteredDocs);
                } catch (error) {
                    return res.sendStatus(500);
                }
            }
        );

    } catch (error) {
        console.log(error);
        // close the client database connection on error
        client.close();
    }
}

main().catch((error) => console.error(error));
