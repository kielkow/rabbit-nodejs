// Consume RabbitMQ data by API
// URL: localhost:5672/api/index.html
// Examples:
// Get Hosts: localhost:5672/api/queues/vhosts
// Get Queue Data: localhost:5672/api/queues/<vhost_name>/<queue_name>

const axios = require('axios');
const amqplib = require('amqplib');
const amqp = require('amqplib/callback_api');

/**
 *  @class RabbitMQ
 *  @type {Object}
 *  @property {string} amqpUrl - Advanced Message Queuing Protocol Rabbit string
 */
class RabbitMQ {
    /**
     * Create define amqpUrl.
     * @param {string} amqpUrl - Advanced Message Queuing Protocol Rabbit string
     */
    constructor({
        amqpUrl
    }) {
        this.amqpUrl = amqpUrl
        this.connection = undefined
        this.channel = undefined
    }
    /**
     * Create a connections.
     * @returns {Promise} Promise object represents rabbitMQ connection.
     */
    async connect() {
        this.connection = await new Promise((resolve, reject) => {
            amqp.connect(this.amqpUrl, (error, connection) => {
                if (error) {
                    return reject(error);
                }
                return resolve(connection)
            })
        })
    }
    /**
     * Create a confirm channel.
     * @returns {Promise} Promise object represents rabbitMQ channel.
     */
    createConfirmChannel() {
        return new Promise((resolve, reject) => {
            this.connection.createConfirmChannel((error, channel) => {
                if (error) {
                    return reject(error);
                }
                return resolve(channel);
            })
        })
    }

    /**
     * Create a channel.
     * @returns {Promise} Promise object represents rabbitMQ channel.
     */
    createChannel() {
        return new Promise((resolve, reject) => {
            this.connection.createChannel((error, channel) => {
                if (error) {
                    return reject(error);
                }
                return resolve(channel);
            })
        })
    }
    /**
     * Send message to queue.
     * @param {string} queueName - queue name
     * @param {Object} data - data to send to queue
     * @returns {Promise} Promise Boolean as long as successful.
     */
    async sendToQueue({
        queueName,
        data
    }) {
        if (!this.connection) {
            throw `rabbitMQ's instance is not connected! `
        }

        if (!this.channel) {
            this.channel = await this.createConfirmChannel();
        }

        if (typeof data !== 'string') {
            data = JSON.stringify(data)
        }

        this.channel.assertQueue(queueName, {
            durable: true
        });

        return new Promise((resolve, reject) => {
            const message = Buffer.from(data)
            const sendMessageToQueue = this.channel.sendToQueue(queueName, message)
            if (!sendMessageToQueue) {
                return reject(`Cannot send message!`);
            }
            return resolve(sendMessageToQueue);
        })
    }

    async consumeQueue({
        queueName
    }) {
        if (!this.connection) throw `rabbitMQ's instance is not connected! `

        if (!this.channel) this.channel = await this.createChannel();

        this.channel.assertQueue(queueName, {
            durable: true
        });
        const messages = []

        this.channel.consume(
            queueName,
            (msg) => {
                if (msg !== null) {
                    messages.push(JSON.parse(msg.content.toString()));
                }
            }, {
                noAck: true
            }
        );

        return messages;
    }

    async getMessageCount({
        queueName
    }) {
        if (!this.connection) {
            throw `rabbitMQ's instance is not connected! `
        }

        if (!this.channel) {
            this.channel = await this.createChannel();
        }
        return new Promise((resolve, reject) => {
            this.channel.assertQueue(queueName, {
                durable: true
            }, (err, data) => {
                if (err) reject(err)
                resolve(data.messageCount)
            });
        })
    }

    /**
     * Close connection.
     */
    async closeConnection() {
        if (this.connection) {
            await this.connection.close()
        }
    }
    /**
     * Close channel.
     */
    async closeChannel() {
        if (this.channel) {
            await this.channel.close()
        }
    }

    testConn() {
        return new Promise(async (resolve, reject) => {
            try {
                const connection = await amqplib.connect(process.env.RABBIT_URL);

                const channel = await connection.createChannel();

                await channel.assertQueue('test-conn');

                console.log('[RABBITMQ]: CONNECTION SUCCESS');

                channel.close();
                connection.close();

                return resolve();
            } catch (error) {
                console.error('[RABBITMQ]: CONNECTION FAIL\n', error);
                return reject(error);
            }
        });
    }

    getQueueInfo(queue) {
        return new Promise(async (resolve, reject) => {
            try {
                const {
                    data
                } = await axios.get(
                    `localhost:5672/api/queues/${process.env.RABBIT_USER}/${queue}`, {
                        auth: {
                            username: process.env.RABBIT_USER,
                            password: process.env.RABBIT_PASS,
                        },
                    }
                );

                console.log(
                    'QUEUE INFO', {
                        messageCount: data.messages,
                        consumerCount: data.consumers,
                    }
                );

                return resolve({
                    messageCount: data.messages,
                    consumerCount: data.consumers,
                });
            } catch (error) {
                console.error('[RABBITMQ]: GET QUEUE INFO FAIL\n', error);
                return reject(error);
            }
        });
    }
}

module.exports = async (ctx) => {
    try {
        const RabbitMq = new RabbitMQ(ctx.credentials);

        await RabbitMq.connect();

        await RabbitMq.sendToQueue({
            queueName: ctx.credentials.queueName,
            data: ctx.body
        });

        setTimeout(async () => {
            await RabbitMq.closeChannel()
            await RabbitMq.closeConnection()
        }, 3000);

        return {
            status: 200,
            message: "Message sended with success!"
        }
    } catch (error) {
        return {
            status: 500,
            message: error.message || error,
            stack: error.stack || ""
        }
    }
}
