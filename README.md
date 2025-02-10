# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/Aiven-Open/karapace/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                  |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| src/karapace/\_\_init\_\_.py                          |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/anonymize\_schemas/\_\_init\_\_.py       |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/anonymize\_schemas/anonymize\_avro.py    |       62 |        0 |       22 |        0 |    100% |           |
| src/karapace/auth.py                                  |      170 |       76 |       34 |        4 |     54% |52-59, 70, 104, 120, 138, 152-158, 162, 167-188, 191-194, 197-227, 230-254, 258-275, 279 |
| src/karapace/avro\_dataclasses/\_\_init\_\_.py        |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/avro\_dataclasses/introspect.py          |       88 |        7 |       44 |        7 |     89% |19, 43, 71, 117, 119, 125, 128 |
| src/karapace/avro\_dataclasses/models.py              |       99 |        6 |       32 |        4 |     92% |21, 106-107, 109, 112, 132 |
| src/karapace/avro\_dataclasses/schema.py              |       33 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/\_\_init\_\_.py                   |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/api.py                            |      303 |       17 |      100 |       13 |     92% |142, 152-154, 168, 172, 279-281, 392, 407->exit, 488->497, 490, 537, 539, 575-579, 679, 695 |
| src/karapace/backup/backends/\_\_init\_\_.py          |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/reader.py                |       50 |        1 |        2 |        0 |     98% |        83 |
| src/karapace/backup/backends/v1.py                    |       13 |        0 |        2 |        0 |    100% |           |
| src/karapace/backup/backends/v2.py                    |       56 |        2 |       14 |        4 |     91% |59, 61, 71->73, 76->78 |
| src/karapace/backup/backends/v3/\_\_init\_\_.py       |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/v3/backend.py            |      155 |       12 |       44 |        8 |     89% |49-53, 62-63, 125, 253, 272, 274, 281, 310, 313 |
| src/karapace/backup/backends/v3/checksum.py           |        7 |        2 |        0 |        0 |     71% |    12, 15 |
| src/karapace/backup/backends/v3/constants.py          |        2 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/v3/errors.py             |       27 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/v3/readers.py            |       47 |        0 |        8 |        0 |    100% |           |
| src/karapace/backup/backends/v3/schema.py             |       64 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/v3/schema\_tool.py       |       79 |       79 |       22 |        0 |      0% |     7-145 |
| src/karapace/backup/backends/v3/writers.py            |       46 |        1 |        4 |        1 |     96% |        24 |
| src/karapace/backup/backends/writer.py                |       48 |        1 |        0 |        0 |     98% |       175 |
| src/karapace/backup/cli.py                            |       80 |       10 |       18 |        2 |     86% |132-147, 156, 168-176, 178-184, 188 |
| src/karapace/backup/encoders.py                       |       20 |        0 |       12 |        0 |    100% |           |
| src/karapace/backup/errors.py                         |       42 |        7 |        0 |        0 |     83% |57, 62, 67, 72, 80, 85, 90 |
| src/karapace/backup/poll\_timeout.py                  |       30 |        0 |        4 |        0 |    100% |           |
| src/karapace/backup/safe\_writer.py                   |       70 |        0 |       12 |        0 |    100% |           |
| src/karapace/backup/topic\_configurations.py          |        9 |        0 |        0 |        0 |    100% |           |
| src/karapace/client.py                                |       85 |        4 |       14 |        1 |     95% |68-69, 79-80 |
| src/karapace/compatibility/\_\_init\_\_.py            |       15 |        2 |        0 |        0 |     87% |     36-41 |
| src/karapace/compatibility/jsonschema/\_\_init\_\_.py |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/compatibility/jsonschema/checks.py       |      370 |       46 |      162 |       21 |     84% |202, 252, 265, 334, 425, 435-446, 521-533, 546-550, 575, 599, 661, 674, 684, 764-776, 787-797, 822-825, 879->877, 881, 885, 895-900, 920 |
| src/karapace/compatibility/jsonschema/types.py        |      110 |        0 |        0 |        0 |    100% |           |
| src/karapace/compatibility/jsonschema/utils.py        |      132 |       22 |       62 |       11 |     80% |32, 46, 51, 105, 127-137, 150, 199-203, 209, 311->310, 330, 339, 355, 367 |
| src/karapace/compatibility/protobuf/\_\_init\_\_.py   |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/compatibility/protobuf/checks.py         |       17 |       13 |        6 |        0 |     17% |     11-25 |
| src/karapace/compatibility/schema\_compatibility.py   |       61 |       25 |       28 |        6 |     49% |41, 48-65, 76, 81, 85->126, 97-124, 130, 138 |
| src/karapace/config.py                                |      202 |       64 |       50 |        5 |     61% |178-186, 238-252, 261, 268-270, 277-279, 284, 296-297, 304-330, 340-359 |
| src/karapace/constants.py                             |        8 |        0 |        0 |        0 |    100% |           |
| src/karapace/coordinator/\_\_init\_\_.py              |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/coordinator/master\_coordinator.py       |       96 |       13 |       18 |        5 |     82% |80-82, 85-89, 99->101, 104, 106-107, 150-152, 177 |
| src/karapace/coordinator/schema\_coordinator.py       |      514 |       42 |      140 |       11 |     91% |198, 213-214, 276-285, 298-301, 321-322, 334-335, 499, 586-591, 611-615, 633, 643-647, 680->exit, 685->688, 699->726, 834, 851, 932, 976-977, 984-985 |
| src/karapace/dataclasses.py                           |       11 |        0 |        2 |        0 |    100% |           |
| src/karapace/dependency.py                            |       42 |       11 |       10 |        3 |     65% |18, 45, 49, 53-54, 58, 65, 68, 71-73 |
| src/karapace/errors.py                                |       40 |        5 |        2 |        1 |     86% |11-12, 65-67 |
| src/karapace/in\_memory\_database.py                  |      264 |       69 |       84 |       11 |     70% |31, 41, 45, 58, 62, 66, 70, 74, 78, 82, 86, 90, 94, 98, 102, 106, 110, 114, 118, 122, 126, 130, 151-164, 169, 176, 187-190, 210, 233, 265-267, 270->exit, 274-275, 288, 291->293, 297-305, 322-325, 342-346, 351->350, 359->exit, 360->362, 386, 399 |
| src/karapace/instrumentation/\_\_init\_\_.py          |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/instrumentation/prometheus.py            |       39 |        0 |        0 |        0 |    100% |           |
| src/karapace/kafka/\_\_init\_\_.py                    |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/kafka/admin.py                           |       74 |        0 |       12 |        0 |    100% |           |
| src/karapace/kafka/common.py                          |       88 |        6 |       18 |        5 |     90% |59, 61, 63, 84, 176, 214 |
| src/karapace/kafka/consumer.py                        |      142 |       15 |       20 |        3 |     89% |49-50, 63, 68, 100, 103-104, 135-136, 162-163, 182-183, 188-189 |
| src/karapace/kafka/producer.py                        |       67 |        2 |        6 |        0 |     97% |     63-64 |
| src/karapace/kafka/types.py                           |        8 |        0 |        0 |        0 |    100% |           |
| src/karapace/kafka\_error\_handler.py                 |       28 |        0 |        6 |        0 |    100% |           |
| src/karapace/kafka\_rest\_apis/\_\_init\_\_.py        |      630 |       69 |      140 |       15 |     88% |98-101, 301-313, 317-319, 480, 484, 491, 494, 505, 510->547, 533-543, 686, 729-730, 741->exit, 755-761, 801, 820, 843, 854, 879-880, 903, 972-973, 986-987, 1007, 1052-1057, 1101, 1160, 1210-1212, 1216-1217, 1221-1223, 1225-1227, 1234-1235, 1250, 1291, 1295->1301 |
| src/karapace/kafka\_rest\_apis/authentication.py      |       64 |        0 |       14 |        0 |    100% |           |
| src/karapace/kafka\_rest\_apis/consumer\_manager.py   |      350 |       38 |       72 |        8 |     89% |111, 204-205, 213->exit, 242-248, 258-259, 291-292, 307-310, 315, 337, 355, 451, 453, 488-489, 492, 494, 522-532, 553-554, 587, 601-602 |
| src/karapace/kafka\_rest\_apis/error\_codes.py        |       19 |        0 |        0 |        0 |    100% |           |
| src/karapace/kafka\_rest\_apis/schema\_cache.py       |       73 |       20 |        2 |        1 |     72% |19, 23, 27, 31, 35, 47, 54-55, 58-59, 73-74, 79, 87, 90-91, 99, 102, 105, 108 |
| src/karapace/kafka\_utils.py                          |       20 |        0 |        0 |        0 |    100% |           |
| src/karapace/karapace.py                              |       59 |        8 |        4 |        1 |     83% |60, 76, 90, 100, 109-112 |
| src/karapace/karapace\_all.py                         |       74 |       14 |       20 |        8 |     77% |33-35, 39-41, 43->49, 52-53, 71-72, 74-75, 80-81, 99, 102->exit |
| src/karapace/key\_format.py                           |       36 |        0 |        6 |        0 |    100% |           |
| src/karapace/messaging.py                             |       57 |       24 |       10 |        1 |     51% |54-56, 60->exit, 64-100, 107-111 |
| src/karapace/offset\_watcher.py                       |       14 |        1 |        0 |        0 |     93% |        24 |
| src/karapace/protobuf/\_\_init\_\_.py                 |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/protobuf/compare\_result.py              |       60 |        1 |        6 |        0 |     98% |        65 |
| src/karapace/protobuf/compare\_type\_lists.py         |       42 |        8 |       22 |        4 |     75% |44, 48, 50-55, 63 |
| src/karapace/protobuf/compare\_type\_storage.py       |      111 |       19 |       42 |       12 |     80% |13-14, 24, 35, 61-63, 95, 99, 102, 107, 111, 114, 118, 126, 139-141, 144 |
| src/karapace/protobuf/encoding\_variants.py           |       43 |       30 |       18 |        2 |     28% |16-32, 36-44, 49, 55-66 |
| src/karapace/protobuf/enum\_constant\_element.py      |       21 |        0 |        2 |        0 |    100% |           |
| src/karapace/protobuf/enum\_element.py                |       51 |        5 |       26 |        4 |     88% |59-60, 73, 75, 78 |
| src/karapace/protobuf/exception.py                    |       25 |        9 |        6 |        1 |     55% |12, 44, 49-55 |
| src/karapace/protobuf/extend\_element.py              |       22 |        0 |        4 |        0 |    100% |           |
| src/karapace/protobuf/extensions\_element.py          |       26 |        1 |        8 |        2 |     91% |28->38, 36 |
| src/karapace/protobuf/field.py                        |        7 |        0 |        0 |        0 |    100% |           |
| src/karapace/protobuf/field\_element.py               |      101 |        8 |       38 |        6 |     90% |110, 113, 143, 149, 160, 167, 172, 175 |
| src/karapace/protobuf/group\_element.py               |       27 |        0 |        6 |        1 |     97% |    33->37 |
| src/karapace/protobuf/io.py                           |      180 |       87 |       44 |        8 |     48% |35, 39-56, 79-84, 89, 92, 100-150, 159-169, 182-187, 215-218, 239->241, 254-268, 298-299, 311->317, 312->311, 318 |
| src/karapace/protobuf/known\_dependency.py            |       21 |        0 |        6 |        1 |     96% |    14->16 |
| src/karapace/protobuf/kotlin\_wrapper.py              |       21 |        1 |        8 |        0 |     97% |        36 |
| src/karapace/protobuf/location.py                     |       27 |        3 |        8 |        3 |     83% |19, 30, 39, 46->50 |
| src/karapace/protobuf/message\_element.py             |      117 |        3 |       62 |        3 |     97% |97-98, 100->exit, 168 |
| src/karapace/protobuf/one\_of\_element.py             |       49 |        0 |       22 |        1 |     99% |    44->48 |
| src/karapace/protobuf/option\_element.py              |       80 |        2 |       30 |        3 |     95% |50, 80->83, 87 |
| src/karapace/protobuf/option\_reader.py               |      104 |       17 |       46 |        4 |     81% |60, 100, 111-115, 121-127, 135-139 |
| src/karapace/protobuf/proto\_file\_element.py         |       95 |        4 |       44 |        3 |     95% |22->exit, 34, 120, 130, 145 |
| src/karapace/protobuf/proto\_normalizations.py        |      115 |        4 |       10 |        2 |     95% |150-153, 210, 221 |
| src/karapace/protobuf/proto\_parser.py                |      383 |        9 |      130 |       10 |     96% |109, 117, 295, 321, 349, 486, 514, 516->519, 553, 616 |
| src/karapace/protobuf/proto\_type.py                  |      130 |       19 |       26 |        5 |     81% |17->19, 122, 146-147, 157, 160-169, 172, 178, 181, 185, 197, 202, 210, 249 |
| src/karapace/protobuf/protobuf\_to\_dict.py           |      185 |      159 |      102 |        0 |      9% |24-27, 31-32, 62, 66-68, 72, 85-132, 143-160, 188-192, 196-226, 230-304, 308-315, 322-331, 345-350 |
| src/karapace/protobuf/protopace/\_\_init\_\_.py       |        1 |        0 |        0 |        0 |    100% |           |
| src/karapace/protobuf/protopace/protopace.py          |       87 |       39 |       16 |        4 |     52% |16, 50, 69-72, 80-103, 153-157, 161-170, 174-183, 187-188 |
| src/karapace/protobuf/reserved\_element.py            |       27 |        1 |       10 |        1 |     95% |        38 |
| src/karapace/protobuf/rpc\_element.py                 |       30 |        0 |        8 |        0 |    100% |           |
| src/karapace/protobuf/schema.py                       |      278 |       14 |      150 |        8 |     94% |76-78, 81-83, 178, 200, 205, 278, 290, 301, 448, 505 |
| src/karapace/protobuf/serialization.py                |      261 |       34 |      172 |       39 |     83% |59, 61, 131, 135, 137, 139, 141, 143, 147, 149, 151, 157, 159, 163, 214, 230->232, 232->234, 258, 271->260, 294, 296, 298, 300, 302, 304, 306, 308, 310, 312, 314, 316, 318, 320, 322, 324, 326, 328, 337->340, 349->346 |
| src/karapace/protobuf/service\_element.py             |       28 |        0 |        8 |        0 |    100% |           |
| src/karapace/protobuf/syntax.py                       |       12 |        1 |        0 |        0 |     92% |        24 |
| src/karapace/protobuf/syntax\_reader.py               |      262 |       13 |      124 |       15 |     92% |53-54, 66, 105, 168->179, 191-192, 214, 218->221, 225->246, 247, 248->259, 252->258, 260, 265->274, 300->323, 313-314, 345, 368 |
| src/karapace/protobuf/type\_element.py                |       26 |        9 |        2 |        1 |     64% |15-17, 33, 36-37, 40-41, 44 |
| src/karapace/protobuf/type\_tree.py                   |       64 |        2 |       16 |        3 |     94% |65, 69->67, 72 |
| src/karapace/protobuf/utils.py                        |       46 |        3 |       20 |        3 |     91% |10, 20, 52 |
| src/karapace/rapu.py                                  |      262 |       54 |       76 |       17 |     76% |84, 88, 103, 107, 140, 234, 243-252, 277-281, 286, 292-301, 307, 314, 317->320, 322->325, 325->328, 335, 341-342, 347-354, 359-364, 371, 374-375, 383, 405-408, 451, 467, 481-483 |
| src/karapace/schema\_models.py                        |      178 |       32 |       40 |        9 |     80% |86-87, 94->99, 97, 129, 153-155, 160-167, 174, 186-195, 209, 219-220, 226-227, 239-251, 337, 343-344, 347, 350-352, 377, 431->exit |
| src/karapace/schema\_reader.py                        |      441 |      102 |       92 |       15 |     75% |200-210, 218-228, 247-255, 266-267, 269-270, 274->276, 314, 317-321, 333-345, 370, 377-378, 387, 430-438, 451->460, 548->551, 555-556, 557->exit, 563-564, 567-572, 578, 582-586, 590-591, 604-606, 620-622, 623->643, 635, 651-652, 674, 691, 694-697, 716-728, 738, 740-742 |
| src/karapace/schema\_references.py                    |       37 |        3 |        2 |        1 |     90% |27, 41, 61 |
| src/karapace/schema\_registry.py                      |      256 |      187 |       78 |        0 |     21% |67, 71, 74-75, 93-104, 107-115, 118-120, 123-131, 134-191, 194-226, 229-236, 239-265, 270-281, 284-285, 302-381, 386-394, 397, 400, 412-427, 430-432, 435-436, 442, 445-447, 454-483, 487-492 |
| src/karapace/schema\_registry\_apis.py                |      559 |      412 |      140 |        5 |     23% |91-92, 103-124, 136, 139-141, 145, 340->342, 343, 346-366, 370, 387-415, 418-438, 447-511, 516-540, 543, 546-550, 553-578, 583-609, 626-651, 661-676, 679-705, 714-723, 728-781, 786-813, 818-888, 893-910, 920-945, 950-957, 968-969, 980, 1000-1021, 1024-1025, 1040-1074, 1079-1185, 1202-1285, 1293-1294, 1307-1319, 1326-1329, 1334-1351, 1366-1378, 1388-1410 |
| src/karapace/schema\_type.py                          |        6 |        0 |        0 |        0 |    100% |           |
| src/karapace/sentry/\_\_init\_\_.py                   |       16 |        3 |        0 |        0 |     81% | 11, 25-26 |
| src/karapace/sentry/sentry\_client.py                 |       23 |        0 |        2 |        1 |     96% |  60->exit |
| src/karapace/sentry/sentry\_client\_api.py            |       13 |        2 |        0 |        0 |     85% |    23, 26 |
| src/karapace/serialization.py                         |      276 |       28 |       88 |       13 |     89% |84, 122, 139, 150, 154, 180-181, 211, 225-226, 233, 247-248, 290-291, 340->343, 374, 379, 450-455, 461-464, 482, 491 |
| src/karapace/statsd.py                                |       61 |        9 |       16 |        4 |     81% |36-38, 62, 71, 73-75, 77 |
| src/karapace/typing.py                                |       83 |        4 |        8 |        2 |     93% |78, 90, 112, 116 |
| src/karapace/utils.py                                 |      127 |       44 |       28 |        3 |     61% |31-33, 45-47, 52, 57, 62, 67, 73-82, 93, 105, 118, 132, 137, 152, 156, 198, 206-209, 228-246 |
| src/karapace/version.py                               |       11 |        2 |        2 |        1 |     77% |       5-6 |
|                                             **TOTAL** | **10923** | **2121** | **3096** |  **380** | **78%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/Aiven-Open/karapace/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/Aiven-Open/karapace/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Aiven-Open/karapace/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/Aiven-Open/karapace/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2FAiven-Open%2Fkarapace%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/Aiven-Open/karapace/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.