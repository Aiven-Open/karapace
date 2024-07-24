# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/Aiven-Open/karapace/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|-------------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| karapace/\_\_init\_\_.py                          |        0 |        0 |        0 |        0 |    100% |           |
| karapace/anonymize\_schemas/\_\_init\_\_.py       |        0 |        0 |        0 |        0 |    100% |           |
| karapace/anonymize\_schemas/anonymize\_avro.py    |       62 |        0 |       22 |        0 |    100% |           |
| karapace/auth.py                                  |      170 |       76 |       48 |        4 |     55% |52-59, 70, 104, 120, 138, 152-158, 162, 167-188, 191-194, 197-227, 230-254, 258-275, 279 |
| karapace/avro\_dataclasses/\_\_init\_\_.py        |        0 |        0 |        0 |        0 |    100% |           |
| karapace/avro\_dataclasses/introspect.py          |       85 |        7 |       44 |        7 |     89% |23, 47, 68, 114, 116, 122, 125 |
| karapace/avro\_dataclasses/models.py              |       98 |        6 |       44 |        4 |     93% |20, 105-106, 108, 111, 131 |
| karapace/avro\_dataclasses/schema.py              |       33 |        0 |        0 |        0 |    100% |           |
| karapace/backup/\_\_init\_\_.py                   |        0 |        0 |        0 |        0 |    100% |           |
| karapace/backup/api.py                            |      298 |       60 |      137 |       10 |     77% |141, 151-153, 167, 171, 278-280, 384, 399->exit, 479->488, 481, 528, 530, 565-575, 600-642, 651-692 |
| karapace/backup/backends/\_\_init\_\_.py          |        0 |        0 |        0 |        0 |    100% |           |
| karapace/backup/backends/reader.py                |       49 |        1 |       18 |        0 |     99% |        82 |
| karapace/backup/backends/v1.py                    |       12 |        0 |        4 |        0 |    100% |           |
| karapace/backup/backends/v2.py                    |       53 |        2 |       30 |        4 |     93% |55, 57, 67->69, 72->74 |
| karapace/backup/backends/v3/\_\_init\_\_.py       |        0 |        0 |        0 |        0 |    100% |           |
| karapace/backup/backends/v3/backend.py            |      154 |       30 |       66 |        8 |     77% |48-52, 61-62, 124, 146-156, 168-180, 252, 271, 273, 280, 309, 312 |
| karapace/backup/backends/v3/checksum.py           |        7 |        2 |        0 |        0 |     71% |    12, 15 |
| karapace/backup/backends/v3/constants.py          |        2 |        0 |        0 |        0 |    100% |           |
| karapace/backup/backends/v3/errors.py             |       27 |        0 |        0 |        0 |    100% |           |
| karapace/backup/backends/v3/readers.py            |       46 |        0 |       12 |        0 |    100% |           |
| karapace/backup/backends/v3/schema.py             |       63 |        0 |       12 |        0 |    100% |           |
| karapace/backup/backends/v3/schema\_tool.py       |       78 |       78 |       26 |        0 |      0% |     7-144 |
| karapace/backup/backends/v3/writers.py            |       46 |        1 |       10 |        1 |     96% |        24 |
| karapace/backup/backends/writer.py                |       47 |        1 |       20 |        0 |     99% |       174 |
| karapace/backup/cli.py                            |       79 |       79 |       26 |        0 |      0% |     7-178 |
| karapace/backup/encoders.py                       |       20 |        0 |       12 |        0 |    100% |           |
| karapace/backup/errors.py                         |       42 |        7 |       14 |        0 |     88% |57, 62, 67, 72, 80, 85, 90 |
| karapace/backup/poll\_timeout.py                  |       30 |        0 |       12 |        0 |    100% |           |
| karapace/backup/safe\_writer.py                   |       67 |        0 |       26 |        0 |    100% |           |
| karapace/backup/topic\_configurations.py          |        8 |        0 |        0 |        0 |    100% |           |
| karapace/client.py                                |       84 |        4 |       26 |        1 |     95% |67-68, 78-79 |
| karapace/compatibility/\_\_init\_\_.py            |       68 |       39 |       30 |        1 |     33% |51-56, 60, 64, 68, 81-174 |
| karapace/compatibility/jsonschema/\_\_init\_\_.py |        0 |        0 |        0 |        0 |    100% |           |
| karapace/compatibility/jsonschema/checks.py       |      370 |       46 |      164 |       21 |     84% |202, 252, 265, 334, 425, 435-446, 521-533, 546-550, 575, 599, 661, 674, 684, 764-776, 787-797, 822-825, 879->877, 881, 885, 895-900, 920 |
| karapace/compatibility/jsonschema/types.py        |      110 |        0 |       10 |        0 |    100% |           |
| karapace/compatibility/jsonschema/utils.py        |      132 |       22 |       64 |       11 |     80% |32, 46, 51, 105, 127-137, 150, 199-203, 209, 311->310, 330, 339, 355, 367 |
| karapace/compatibility/protobuf/\_\_init\_\_.py   |        0 |        0 |        0 |        0 |    100% |           |
| karapace/compatibility/protobuf/checks.py         |       17 |       13 |        6 |        0 |     17% |     11-25 |
| karapace/config.py                                |      194 |       64 |       50 |        5 |     59% |163-171, 223-237, 246, 253-255, 262-264, 269, 281-282, 289-315, 325-344 |
| karapace/constants.py                             |        8 |        0 |        0 |        0 |    100% |           |
| karapace/coordinator/\_\_init\_\_.py              |        0 |        0 |        0 |        0 |    100% |           |
| karapace/coordinator/master\_coordinator.py       |       65 |        9 |       12 |        3 |     84% |52-54, 57-61, 107-109, 124->126, 126->exit |
| karapace/coordinator/schema\_coordinator.py       |      476 |       38 |      140 |        9 |     91% |221-230, 243-246, 266-267, 279-280, 496-501, 519-523, 541, 551-555, 588->exit, 593->596, 607->634, 742, 759, 840, 884-885, 892-893 |
| karapace/dataclasses.py                           |       11 |        0 |        4 |        0 |    100% |           |
| karapace/dependency.py                            |       31 |        8 |        6 |        1 |     70% |15, 42, 45, 52, 55, 58-60 |
| karapace/errors.py                                |       38 |        5 |        2 |        1 |     85% |11-12, 65-67 |
| karapace/in\_memory\_database.py                  |      186 |       83 |      102 |        7 |     46% |45-58, 61-64, 67-72, 81-84, 95, 98, 104, 127, 156-158, 161->exit, 165-166, 179, 182->184, 188-196, 199-207, 213-216, 222, 225-226, 233-237, 240-242, 245-246, 267-272, 275-276, 279-283 |
| karapace/instrumentation/\_\_init\_\_.py          |        0 |        0 |        0 |        0 |    100% |           |
| karapace/instrumentation/prometheus.py            |       38 |        0 |       10 |        0 |    100% |           |
| karapace/kafka/\_\_init\_\_.py                    |        0 |        0 |        0 |        0 |    100% |           |
| karapace/kafka/admin.py                           |       75 |        0 |       13 |        0 |    100% |           |
| karapace/kafka/common.py                          |       86 |        4 |       17 |        3 |     93% |54, 75, 167, 205 |
| karapace/kafka/consumer.py                        |      141 |       12 |       26 |        2 |     92% |48-49, 62, 67, 134-135, 161-162, 181-182, 187-188 |
| karapace/kafka/producer.py                        |       67 |        2 |        6 |        0 |     97% |     63-64 |
| karapace/kafka/types.py                           |        8 |        0 |        0 |        0 |    100% |           |
| karapace/kafka\_rest\_apis/\_\_init\_\_.py        |      630 |       69 |      188 |       19 |     88% |98-101, 129->107, 301-313, 317-319, 480, 484, 491, 494, 505, 510->509, 533-543, 686, 729-730, 741->exit, 755-761, 801, 820, 843, 854, 879-880, 903, 972-973, 986-987, 1007, 1052-1057, 1101, 1160, 1210-1212, 1216-1217, 1221-1223, 1225-1227, 1234-1235, 1250, 1291, 1295->1301 |
| karapace/kafka\_rest\_apis/authentication.py      |       64 |        0 |       18 |        0 |    100% |           |
| karapace/kafka\_rest\_apis/consumer\_manager.py   |      344 |       38 |      118 |        9 |     88% |112, 195-196, 204->exit, 233-239, 249-250, 282-283, 298-301, 306, 324, 342, 438, 440, 475-476, 479, 481, 509-519, 540-541, 574, 588-589 |
| karapace/kafka\_rest\_apis/error\_codes.py        |       19 |        0 |        0 |        0 |    100% |           |
| karapace/kafka\_rest\_apis/schema\_cache.py       |       72 |       20 |       12 |        1 |     75% |18, 22, 26, 30, 34, 46, 53-54, 57-58, 72-73, 78, 86, 89-90, 98, 101, 104, 107 |
| karapace/kafka\_utils.py                          |       20 |        0 |        4 |        0 |    100% |           |
| karapace/karapace.py                              |       49 |        5 |       14 |        1 |     90% |51, 67, 81, 91, 99 |
| karapace/karapace\_all.py                         |       55 |        9 |       16 |        5 |     80% |36-37, 43-44, 46-47, 52-53, 71, 74->exit |
| karapace/key\_format.py                           |       31 |        0 |        6 |        0 |    100% |           |
| karapace/messaging.py                             |       57 |       24 |       10 |        1 |     51% |54-56, 60->exit, 64-100, 107-111 |
| karapace/offset\_watcher.py                       |       14 |        1 |        6 |        0 |     95% |        24 |
| karapace/protobuf/\_\_init\_\_.py                 |        0 |        0 |        0 |        0 |    100% |           |
| karapace/protobuf/compare\_result.py              |       61 |        1 |       10 |        0 |     99% |        66 |
| karapace/protobuf/compare\_type\_lists.py         |       42 |        8 |       22 |        4 |     75% |44, 48, 50-55, 63 |
| karapace/protobuf/compare\_type\_storage.py       |      111 |       19 |       48 |       12 |     78% |13-14, 24, 35, 61-63, 95, 99, 102, 107, 111, 114, 118, 126, 139-141, 144 |
| karapace/protobuf/encoding\_variants.py           |       44 |       30 |       18 |        2 |     29% |17-33, 37-45, 50, 56-67 |
| karapace/protobuf/enum\_constant\_element.py      |       21 |        0 |        4 |        0 |    100% |           |
| karapace/protobuf/enum\_element.py                |       51 |        5 |       26 |        4 |     88% |59-60, 73, 75, 78 |
| karapace/protobuf/exception.py                    |       25 |        9 |        6 |        1 |     55% |12, 44, 49-55 |
| karapace/protobuf/extend\_element.py              |       22 |        0 |        6 |        0 |    100% |           |
| karapace/protobuf/extensions\_element.py          |       26 |        1 |       10 |        2 |     92% |28->38, 36 |
| karapace/protobuf/field.py                        |        7 |        0 |        0 |        0 |    100% |           |
| karapace/protobuf/field\_element.py               |      101 |        8 |       40 |        6 |     90% |110, 113, 143, 149, 160, 167, 172, 175 |
| karapace/protobuf/group\_element.py               |       27 |        0 |        8 |        1 |     97% |    33->37 |
| karapace/protobuf/io.py                           |      179 |       87 |       50 |        8 |     46% |34, 38-55, 78-83, 88, 91, 99-149, 158-168, 181-186, 214-217, 238->240, 253-267, 297-298, 310->316, 311->310, 317 |
| karapace/protobuf/known\_dependency.py            |       21 |        0 |       10 |        1 |     97% |    14->16 |
| karapace/protobuf/kotlin\_wrapper.py              |       21 |        1 |       10 |        0 |     97% |        36 |
| karapace/protobuf/location.py                     |       27 |        3 |        8 |        3 |     83% |19, 30, 39, 46->50 |
| karapace/protobuf/message\_element.py             |      117 |        3 |       62 |        3 |     97% |97-98, 100->exit, 168 |
| karapace/protobuf/one\_of\_element.py             |       49 |        0 |       22 |        0 |    100% |           |
| karapace/protobuf/option\_element.py              |       80 |        2 |       32 |        3 |     96% |50, 80->83, 87 |
| karapace/protobuf/option\_reader.py               |      104 |       17 |       50 |        4 |     81% |60, 100, 111-115, 121-127, 135-139 |
| karapace/protobuf/proto\_file\_element.py         |       94 |        4 |       46 |        3 |     95% |20->exit, 32, 118, 128, 143 |
| karapace/protobuf/proto\_normalizations.py        |      116 |        4 |       12 |        2 |     95% |151-154, 211, 222 |
| karapace/protobuf/proto\_parser.py                |      383 |        9 |      138 |       10 |     96% |109, 117, 223, 321, 349, 486, 514, 516->519, 553, 616 |
| karapace/protobuf/proto\_type.py                  |      130 |       19 |       42 |        5 |     83% |17->19, 122, 146-147, 157, 160-169, 172, 178, 181, 185, 197, 202, 210, 249 |
| karapace/protobuf/protobuf\_to\_dict.py           |      186 |      159 |      108 |        0 |      9% |25-28, 32-33, 63, 67-69, 73, 86-133, 144-161, 189-193, 197-227, 231-305, 309-316, 323-332, 346-351 |
| karapace/protobuf/reserved\_element.py            |       27 |        1 |       12 |        1 |     95% |        38 |
| karapace/protobuf/rpc\_element.py                 |       30 |        0 |        8 |        0 |    100% |           |
| karapace/protobuf/schema.py                       |      278 |       14 |      156 |        8 |     94% |76-78, 81-83, 178, 200, 205, 278, 290, 301, 448, 505 |
| karapace/protobuf/serialization.py                |      254 |       35 |      172 |       40 |     82% |58, 60, 116, 120, 122, 124, 126, 128, 132, 134, 136, 142, 144, 148, 199, 208, 215->217, 217->219, 243, 256->245, 279, 281, 283, 285, 287, 289, 291, 293, 295, 297, 299, 301, 303, 305, 307, 309, 311, 313, 322->325, 334->331 |
| karapace/protobuf/service\_element.py             |       28 |        0 |       10 |        0 |    100% |           |
| karapace/protobuf/syntax.py                       |       12 |        1 |        2 |        0 |     93% |        24 |
| karapace/protobuf/syntax\_reader.py               |      263 |       14 |      124 |       15 |     92% |53-54, 66, 105-106, 169->180, 192-193, 215, 219->222, 226->247, 248, 249->260, 253->259, 261, 266->275, 301->324, 314-315, 346, 369 |
| karapace/protobuf/type\_element.py                |       25 |        9 |        4 |        1 |     66% |14-16, 32, 35-36, 39-40, 43 |
| karapace/protobuf/type\_tree.py                   |       65 |        2 |       36 |        3 |     95% |66, 70->68, 73 |
| karapace/protobuf/utils.py                        |       46 |        3 |       20 |        3 |     91% |10, 20, 52 |
| karapace/rapu.py                                  |      259 |       55 |       96 |       19 |     75% |84, 88, 103, 107, 140, 143, 231, 240-249, 274-278, 283, 289-298, 304, 311, 314->317, 319->322, 322->325, 332, 338-339, 344-351, 356-361, 368, 371-372, 380, 402-405, 448, 464, 478-480 |
| karapace/schema\_models.py                        |      167 |       32 |       62 |       10 |     81% |79->84, 82, 114, 121-122, 138-140, 145-152, 159, 171-180, 193, 203-204, 210-211, 218-230, 314, 320-321, 324, 327-329, 354, 406->exit |
| karapace/schema\_reader.py                        |      381 |      125 |      124 |       15 |     62% |183-192, 200-209, 228-236, 261, 264-268, 273, 279-291, 316, 325, 341->344, 361-367, 381->390, 459->462, 466-467, 468->exit, 473-483, 486-497, 501-502, 515-517, 531-552, 562-563, 575-576, 587-589, 595, 604, 611, 614-617, 628-648, 654-662 |
| karapace/schema\_references.py                    |       36 |        4 |        8 |        1 |     89% |26, 40, 57, 60 |
| karapace/schema\_registry.py                      |      244 |      181 |       94 |        0 |     20% |64, 68, 71-72, 92-101, 104-112, 115-117, 120-128, 131-188, 191-225, 228-235, 238-264, 269-280, 283-284, 301-399, 404-412, 415, 418, 430-445, 448-450, 453-454, 460, 463-465 |
| karapace/schema\_registry\_apis.py                |      540 |      404 |      187 |        6 |     20% |90-91, 102-118, 130, 133-135, 139, 145, 148, 335->337, 338, 341-361, 365, 382-455, 458-478, 487-551, 556-580, 583, 586-590, 593-618, 623-649, 666-691, 701-716, 719-728, 733-786, 791-818, 823-893, 898-915, 925-950, 955-962, 973-974, 984-995, 1005-1026, 1029-1030, 1045-1079, 1084-1189, 1206-1288, 1296-1297, 1310-1322, 1329-1332, 1337-1354, 1357 |
| karapace/schema\_type.py                          |        6 |        0 |        2 |        0 |    100% |           |
| karapace/sentry/\_\_init\_\_.py                   |       16 |        3 |        0 |        0 |     81% | 11, 25-26 |
| karapace/sentry/sentry\_client.py                 |       21 |        0 |        2 |        1 |     96% |  58->exit |
| karapace/sentry/sentry\_client\_api.py            |       13 |        2 |        0 |        0 |     85% |    23, 26 |
| karapace/serialization.py                         |      275 |       28 |      106 |       14 |     89% |83, 121, 138, 149, 153, 179-180, 210, 224-225, 232, 246-247, 289-290, 339->342, 373, 378, 449-454, 460-463, 481, 490 |
| karapace/statsd.py                                |       62 |        9 |       18 |        4 |     81% |42-44, 68, 77, 79-81, 83 |
| karapace/typing.py                                |       67 |        3 |       22 |        3 |     93% |71, 73, 85 |
| karapace/utils.py                                 |      130 |       44 |       54 |        3 |     67% |30-32, 44-46, 51, 56, 61, 66, 72-81, 92, 104, 117, 131, 136, 151, 155, 197, 205-208, 227-245 |
| karapace/version.py                               |        1 |        0 |        0 |        0 |    100% |           |
|                                         **TOTAL** | **10427** | **2223** | **3770** |  **360** | **76%** |           |


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