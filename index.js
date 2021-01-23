/**
 * TP elastic search Janvier 2021
 * Baptiste
 * BERTRAND-RAPELLO
 */

'use strict'

const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })

async function myQuery() {
    console.log("Test requete")
    const result = await client.search({
        index: 'person-v2'
    })

    console.log(result);

}

async function searchRequest() {
    console.log("request TP 4 recherche")
    const resNumFemale = await client.search({
        index: 'person-v2',
        body: {
            query: {
                match: { "gender": "female" }
            }
        }
    })

    console.log(`-> il y a : ${resNumFemale.body.hits.total.value} femme`)

    const resGreaterThanTwenty = await client.search({
        index: 'person-v2',
        body: {
            query: {
                range: {
                    age: { "gte": 20 }
                }
            }
        }
    })

    console.log(`-> il y a : ${resGreaterThanTwenty.body.hits.total.value} personne de plus de 20 ans`)

    const resManGreaterThanTwenty = await client.search({
        index: 'person-v2',
        body: {
            query: {
                bool:
                {
                    must: [
                        { match: { "gender": "male" } },
                        {
                            range: { age: { "gte": 20 } }
                        }
                    ]
                }
            }
        }
    })

    console.log(`-> il y a : ${resManGreaterThanTwenty.body.hits.total.value} homme de plus de 20 ans`)

    const resMoreThanTwentyAndBalanceGreaterThan = await client.search({
        index: 'person-v2',
        body: {
            query: {
                bool:
                {
                    must: [
                        {
                            range: { balance: { "gte": 1000, "lte": 2000 } }
                        }
                    ]
                }
            }
        }
    })

    console.log(`-> il y a : ${resMoreThanTwentyAndBalanceGreaterThan.body.hits.total.value} qui ont une balance entre 1000$ et 2000$`)

    //celle du geopoint
    var resGeoDistance = ""
    try {
        resGeoDistance = await client.search({
            index: 'person-v4',
            body: {
                query: {
                    bool: {
                        must: {
                            match_all: {}
                        },
                        filter: {
                            geo_distance: {
                                distance: "10km",
                                location: {
                                    lat: 48,
                                    lon: 2.35
                                }
                            }
                        }
                    }
                }
            }
        })
    } catch (err) {
        console.log(err)
    }
    console.log(`-> il y a : ${resGeoDistance.body.hits.total.value} personnes qui vivent a 10km de Paris`)
}

async function agregateRequest() {
    console.log("TP5 sdk : agregation")
    const resAverageAge = await client.search({
        index: 'person-v4',
        body: {
            aggs: {
                avg_age: {
                    avg: {
                        field: "age"
                    }
                }
            }
        }
    })
    console.log(`5-> l'age moyen des personnes indexé est de : ${resAverageAge.body.aggregations.avg_age.value} ans`);

    var resAverageSex = ""
    try {
        resAverageSex = await client.search({
            index: 'person-v4',
            body: {
                aggs: {
                    gender_aggs: {
                        terms: {
                            field: "gender"
                        }
                    }
                }
            }
        })
    } catch (err) {
        console.log(err)
    }

    console.log("5-> les moyennes d'homme et de femme :")
    resAverageSex.body.aggregations.gender_aggs.buckets.forEach(element => {
        console.log(`key : ${element.key} -> ${element.doc_count}`);
    });

    var resAggsEyesAndSex = ""
    try {
        resAggsEyesAndSex = await client.search({
            index: 'person-v4',
            body: {
                aggs: {
                    gender_aggs: {
                        terms: {
                            field: "gender"
                        },
                        aggs: {
                            eyesclor_aggs: {
                                terms: {
                                    field: "eyeColor"
                                }
                            }
                        }
                    }
                }
            }
        })
    }
    catch (err) {
        console.log("ERROR")
        console.log(err)
    }
    console.log(resAggsEyesAndSex)
    console.log("5-> l'agrégation des personnes en fopnction du sexe et de la couleur des yeux :")
    resAggsEyesAndSex.body.aggregations.gender_aggs.buckets.forEach(element => {
        console.log(`key : ${element.key} = `);
        element.eyesclor_aggs.buckets.forEach(ele => {
            console.log(`\tkey : ${ele.key} -> ${ele.doc_count}`);
        });
    });

    var resAggsSexAndRegistery = ""
    try {
        resAggsSexAndRegistery = await client.search({
            index: 'person-v4',
            body: {
                aggs: {
                    gender_aggs: {
                        terms: {
                            field: "gender"
                        },
                        aggs: {
                            date_registered: {
                                date_histogram: {
                                    field: "registered",
                                    calendar_interval: "year"
                                }
                            }
                        }
                    }
                }
            }
        })
    }
    catch (err) {
        console.log("ERROR")
        console.log(err)
    }
    console.log("5-> l'agrégation des personnes en fopnction du sexe et de l'année d'enregistrement :")
    resAggsSexAndRegistery.body.aggregations.gender_aggs.buckets.forEach(element => {
        console.log(`key : ${element.key} = `);
        element.date_registered.buckets.forEach(ele => {
            console.log(`\tkey : ${ele.key_as_string.split('-')[0]} -> ${ele.doc_count}`);
        });
    });

}

//myQuery()
//tp 4
searchRequest()
//tp 5
agregateRequest()