<?php
require 'vendor/autoload.php';
require_once 'vendor/lincanbin/php-pdo-mysql-class/src/PDO.class.php';

use Elasticsearch\ClientBuilder;

class es
{
    private $client;
    private $indexName;

    public function __construct($index)
    {
        $this->client = ClientBuilder::create()->build();
        $this->indexName = $index;
    }

    public function createIndex($param) {

        if ($this->client->indices()->exists(['index' => $param['index']])) {
            $this->client->indices()->delete(['index' => $param['index']]);
        }
        return $this->client->indices()->create($param);
    }

    public function fillIndex()
    {
        $DB = new DB('127.0.0.1', 3306, 'elastic_test', 'root', '');
        $resQuery = $DB->query('select 
              base.ID, 
              base.NAME, 
              base.DATE_CREATE,
              props.PROPERTY_10 AS ORG,
              props.PROPERTY_13 AS CATEGORY,
              props.PROPERTY_7 AS PRICE
            from base 
            INNER JOIN props ON props.IBLOCK_ELEMENT_ID = base.id
              
              order by base.ID desc');

        $i = 1;
        foreach ($resQuery as $buy) {
            $params['body'][] = [
                'index' => [
                    '_index' => $this->indexName,
                    '_id' => $buy['ID'],
                ]
            ];

            $params['body'][] =  [
                'NAME' => $buy['NAME'],
                'DATE_CREATE' => $buy['DATE_CREATE'],
                'ORG' => $buy['ORG'],
                'CATEGORY' => $buy['CATEGORY'],
                'PRICE' => $buy['PRICE']
            ];
            $i++;
            // Every 1000 documents stop and send the bulk request
            if ($i % 1000 == 0) {
                $responses = $this->client->bulk($params);

                // erase the old bulk request
                $params = ['body' => []];
                $i = 1;
                // unset the bulk response when you are done to save memory
                unset($responses);
            }
        }
        return $this->client->bulk($params);
    }
}

$clientHelper = new es('buy');
//echo "<pre>"; print_r($clientHelper->fillIndex()); echo "</pre>";die();

$paramsIndex = [
    'index' => 'buy',
    'body' => [
        "settings" => [
            'number_of_shards' => 1,
                "analysis" => [
                    "filter" => [
                        "ru_stop" => [
                            "type" => "stop",
                            "stopwords" => "_russian_"
                        ],
                        "ru_stemmer" => [
                            "type" => "stemmer",
                            "language" => "russian"
                        ],
                        "my_length" => [
                            "type" => "length",
                            "min" => 3
                        ],
                    ],
                    "analyzer" => [
                        "my_synonyms" => [
                            "tokenizer" => "standard",
                            "filter" => [
                                "lowercase",
                                "apostrophe",
                                "classic",
                                "my_length",
                                "ru_stop",
                                "ru_stemmer"
                            ]
                        ]
                    ]
                ]
        ],
        'mappings' => [
            'properties' => [
                'ID' => [
                    'type' => 'integer'
                ],
                'NAME' => [
                    'type' => 'text',
                    "analyzer" => "russian"
                ],
                'DATE_CREATE' => [
                    "type" => "date",
                    "format" => "yyyy-MM-dd HH:mm:ss"
                ],
                'ORG' => [
                    "type" => "integer"
                ],
                'CATEGORY' => [
                    "type" => "integer"
                ],
                'PRICE' => [
                    "type" => "double"
                ]
            ]
        ]
    ]
];



$client = ClientBuilder::create()->build();
//$client->indices()->create($paramsIndex);
//$res = $client->indices()->create($paramsIndex);
//echo "<pre>"; print_r($res); echo "</pre>";die();



$params = [
//    'scroll' => '30s',          // how long between scroll requests. should be small!
    'size'   => 500,             // how many results *per shard* you want back
    'index'  => 'buy',
    'body'   => [
        'query' => [
            'bool' => [
                "must" => [
                    ['match' => ['NAME' => 'ст.листовая хк легир отож 30хгса-бт-пв-о г11268-76 1,2х1200х2000']],
                ],
                "must_not" => [
                    "term" => ["ORG" => 143]
                ],
                'filter' => [
                    ['range' => ['DATE_CREATE' => [
                        "gte" => "2019-01-01 00:00:00",
						"lte" => "2019-05-01 00:00:00"
                    ]]],
                    [ "term" => ["CATEGORY" => 427]]
                ]
            ]
        ]
    ]
];
$response = $client->search($params);
echo "<pre>"; print_r($response); echo "</pre>";

