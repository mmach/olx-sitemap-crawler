var amqp = require('amqplib/callback_api');
var Crawler = require("crawler");
const redis = require('async-redis');
var Promise = require('bluebird');


var c = new Crawler({
    maxConnections: 10,
    retries: 30,
    retryTimeout: 60000,

});



// Queue just one URL, with default callback


// Queue some HTML code directly without grabbing (mostly for tests)
amqp.connect(process.env.AMQP ? process.env.AMQP : 'amqp://mq2-justshare.e4ff.pro-eu-west-1.openshiftapps.com', async function (error0, connection) {
    if (error0) {
        console.log(error0)
        throw error0;
    }

    connection.createChannel(function (error1, channel) {

        if (error1) {
            throw error1;
        }

        var queue = 'olx-sitemap-crawler';


        channel.prefetch(5);

        console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);
        channel.consume(queue, function (msg) {
            // throw msg;
            //   var secs = msg.content.toString().split('.').length - 1;
            let obj = msg.content.toString();
            console.log("TRYING TO RETRIVE DATA: " + obj);
            c.queue({
                uri: obj,
                forceUTF8: false,
                headers: {
                    "Content-Type": "application/json",
                    "sec-fetch-site": "same-origin",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-user": "?1",
                    "upgrade-insecure-requests": 1,
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3"
                },
                callback: async function (error, res, done) {
                    if (error) {
                        console.log("#############ERROR############")
                        console.log(error);
                        setTimeout(() => {
                            channel.nack(msg);
                            done();
                        }, 60000)
                    } else {
                        var $ = res.$;

                        let offer = $('table[data-id]')
                        let stopCrawling = $('a[data-cy="page-link-next"]').attr('href') == undefined ? true : false;
                        let itemsToSend = [];

                        Object.keys(offer).filter(item => {
                            return isNaN(item) == false
                        }).map(item => {
                            let tbody = offer[item].children.filter(el => { return el.name == 'tbody' })[0];
                            let tr = tbody.children.filter(el => { return el.name == 'tr' });
                            //console.log(tr)
                            tr.map(trElement => {
                                //console.log(trElement)
                                trElement.children.filter(el => {
                                    if (el.name == 'td' && el.attribs.valign == 'bottom' && el.attribs.class == 'bottom-cell') {
                                        el.children.filter(dataEl => {
                                            if (dataEl.name == 'div') {
                                                dataEl.children.filter(p => {
                                                    if (p.name == 'p') {
                                                        p.children.filter(small => {
                                                            if (small.name == 'small') {
                                                                small.children.filter(span => {
                                                                    if (span.name == 'span') {
                                                                        if (span.children.filter(i => {
                                                                            return i.name == 'i' && i.attribs["data-icon"] == "clock"
                                                                        }).length > 0) {
                                                                            if (new Date().getHours() > 3) {
                                                                                let hour = new Date().getHours()
                                                                                if (span.children[2].data.includes('dzisiaj')) {
                                                                                    let offerHour = Number(span.children[2].data.split(' ')[1].split(':')[0]);
                                                                                    if (offerHour < hour) {
                                                                                        stopCrawling = true;
                                                                                        return;
                                                                                    }
                                                                                } else {
                                                                                    stopCrawling = true;
                                                                                    return;

                                                                                }

                                                                            } else {
                                                                                if (span.children[2].data.includes('wczoraj')) {
                                                                                    let offerHour = Number(span.children[2].data.split(' ')[1].split(':')[0]);
                                                                                    if (offerHour <= 22) {
                                                                                        stopCrawling = true
                                                                                        return;

                                                                                    }

                                                                                } else {
                                                                                    stopCrawling = true
                                                                                    return;

                                                                                }
                                                                            }

                                                                            tr.map(trChildren => {
                                                                                trChildren.children.map(td => {
                                                                                    if (td.name == 'td' && td.attribs.rowspan == '2') {

                                                                                        td.children.forEach(a => {

                                                                                            if (a.name == 'a') {



                                                                                                //console.log(a.attribs.href)
                                                                                                //console.log(span.children[2].data)
                                                                                                itemsToSend.push(a.attribs.href);

                                                                                                //     c_items.queue({
                                                                                                //         uri: a.attribs.href,
                                                                                                //         forceUTF8: false,
                                                                                                //         headers: {
                                                                                                //              "Content-Type": "application/json",
                                                                                                //              "sec-fetch-site": "same-origin",
                                                                                                //              "sec-fetch-mode": "navigate",
                                                                                                //              "sec-fetch-user": "?1",
                                                                                                //              "upgrade-insecure-requests": 1,
                                                                                                //              "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3"


                                                                                                //           }

                                                                                                //      })

                                                                                            }
                                                                                        })
                                                                                    }
                                                                                });
                                                                            })
                                                                        }
                                                                        //console.log(span.children[2].data.includes('dzisiaj'))

                                                                    }
                                                                })
                                                            }
                                                        })
                                                    }
                                                })
                                            }
                                        })
                                    }
                                })
                            })
                        });


                        const CONN_URL = process.env.AMQP ? process.env.AMQP : 'amqp://mq2-justshare.e4ff.pro-eu-west-1.openshiftapps.com';
                        let ch = null;

                        amqp.connect(CONN_URL, function (err, conn) {
                            if (err) {
                                console.log(err);
                                setTimeout(() => {
                                    channel.nack(msg);
                                    done();
                                }, 60000)
                                return;
                            }
                            let crawlerProm = new Promise((res, rej) => {
                                conn.createChannel(async function (err2, channel2) {
                                    if (err2) {
                                        console.log(err2);
                                        rej()
                                        return;
                                    } ch = channel2;
                                    channel2.assertQueue('olx-sitemap-crawler', {
                                        durable: true
                                    });
                                    if (stopCrawling == true) {
                                        res()
                                        return;
                                    } else {
                                        ch.sendToQueue('olx-sitemap-crawler', new Buffer($('a[data-cy="page-link-next"]').attr('href')), { persistent: true });
                                        res()



                                    }

                                })
                            });

                            var client = null;
                            try {
                                client = redis.createClient(
                                    {
                                        port: '6379',
                                        host: '10.131.8.184',
                                        auth_pass: 'test',

                                    })
                            } catch (ex) {
                                console.log(ex);

                            }
                            let newItemProm = new Promise((res, rej) => {
                                conn.createChannel(async function (err2, channel2) {
                                    if (err2) {
                                        console.log(err2);
                                        rej();
                                    } ch = channel2;
                                    channel2.assertQueue('olx-link-items-single', {
                                        durable: true
                                    });


                                    let promList = itemsToSend.filter(item => {
                                        return item.includes('https://www.olx.pl')
                                    }).map(async item => {
                                        try {
                                            //let result = await pool.request().input('link', sql.Text, item.split('#')[0]).input('integration_name', sql.Text, 'OLX_PL').execute(`INSERT_Link`)
                                            let reply = await client.get(item.split('#')[0])
                                            if (reply) {
                                                console.log(reply)
                                                console.log('Duplicates: ' + item)
                                                return;
                                            } else {
                                                await client.set(item.split('#')[0], 'OLX_PL');
                                                await client.expire(item.split('#')[0], 60 * 60 * 3);
                                                await ch.sendToQueue('olx-link-items-single', new Buffer(item.split('#')[0]), { persistent: true });
                                                return

                                            }



                                        } catch (err) {
                                            console.log(err);
                                            //throw err;
                                            rej();

                                        }

                                    });

                                    try {
                                        if (promList.length > 0) {
                                            await Promise.mapSeries(promList, (item) => {
                                                return;
                                            });
                                            //  await client.quit();

                                            setTimeout(() => {
                                                res();
                                            }, 1000)
                                        } else {
                                            res();
                                        }
                                    } catch (err) {
                                        client.quit();
                                        console.log(err);
                                        rej();
                                    }

                                    //  let queue = await addToQueue();
                                    //   console.log(queue);
                                    //queue.forEach(item => {
                                    //  });
                                    //  console.log(test);



                                });


                            });
                            Promise.all([crawlerProm, newItemProm]).then(succ => {
                                channel.ack(msg);
                                setTimeout(() => {
                                    conn.close();
                                    //  ch.close();
                                    done();
                                }, 1000)
                            }, err => {
                                console.log(err);
                                channel.nack(msg);
                                setTimeout(() => {
                                    done();
                                }, 6000)
                            })

                        });

                    }
                }


                // console.log(" [x] Received %s", msg.content.toString());

            }, {
                noAck: false
            });
        });
    });
});