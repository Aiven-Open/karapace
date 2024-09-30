# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/Aiven-Open/karapace/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                  |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| src/karapace/\_\_init\_\_.py                          |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/anonymize\_schemas/\_\_init\_\_.py       |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/anonymize\_schemas/anonymize\_avro.py    |       62 |        0 |       22 |        0 |    100% |           |
| src/karapace/auth.py                                  |      170 |       76 |       48 |        4 |     55% |52-59, 70, 104, 120, 138, 152-158, 162, 167-188, 191-194, 197-227, 230-254, 258-275, 279 |
| src/karapace/avro\_dataclasses/\_\_init\_\_.py        |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/avro\_dataclasses/introspect.py          |       85 |        7 |       44 |        7 |     89% |23, 47, 68, 114, 116, 122, 125 |
| src/karapace/avro\_dataclasses/models.py              |       98 |        6 |       44 |        4 |     93% |20, 105-106, 108, 111, 131 |
| src/karapace/avro\_dataclasses/schema.py              |       33 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/\_\_init\_\_.py                   |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/api.py                            |      299 |       17 |      137 |       13 |     92% |142, 152-154, 168, 172, 279-281, 385, 400->exit, 480->489, 482, 529, 531, 567-571, 671, 687 |
| src/karapace/backup/backends/\_\_init\_\_.py          |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/reader.py                |       49 |        1 |       18 |        0 |     99% |        82 |
| src/karapace/backup/backends/v1.py                    |       12 |        0 |        4 |        0 |    100% |           |
| src/karapace/backup/backends/v2.py                    |       55 |        2 |       30 |        4 |     93% |58, 60, 70->72, 75->77 |
| src/karapace/backup/backends/v3/\_\_init\_\_.py       |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/v3/backend.py            |      154 |       12 |       66 |        8 |     90% |48-52, 61-62, 124, 252, 271, 273, 280, 309, 312 |
| src/karapace/backup/backends/v3/checksum.py           |        7 |        2 |        0 |        0 |     71% |    12, 15 |
| src/karapace/backup/backends/v3/constants.py          |        2 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/v3/errors.py             |       27 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/v3/readers.py            |       46 |        0 |       12 |        0 |    100% |           |
| src/karapace/backup/backends/v3/schema.py             |       63 |        0 |       12 |        0 |    100% |           |
| src/karapace/backup/backends/v3/schema\_tool.py       |       86 |       86 |       32 |        0 |      0% |     7-160 |
| src/karapace/backup/backends/v3/writers.py            |       46 |        1 |       10 |        1 |     96% |        24 |
| src/karapace/backup/backends/writer.py                |       47 |        1 |       20 |        0 |     99% |       174 |
| src/karapace/backup/cli.py                            |       79 |       10 |       26 |        3 |     86% |122-137, 146, 158-166, 168-174, 178 |
| src/karapace/backup/encoders.py                       |       20 |        0 |       12 |        0 |    100% |           |
| src/karapace/backup/errors.py                         |       42 |        7 |       14 |        0 |     88% |57, 62, 67, 72, 80, 85, 90 |
| src/karapace/backup/poll\_timeout.py                  |       30 |        0 |       12 |        0 |    100% |           |
| src/karapace/backup/safe\_writer.py                   |       69 |        0 |       26 |        0 |    100% |           |
| src/karapace/backup/topic\_configurations.py          |        8 |        0 |        0 |        0 |    100% |           |
| src/karapace/client.py                                |       84 |        4 |       26 |        1 |     95% |67-68, 78-79 |
| src/karapace/compatibility/\_\_init\_\_.py            |       68 |       39 |       30 |        1 |     33% |51-56, 60, 64, 68, 81-174 |
| src/karapace/compatibility/jsonschema/\_\_init\_\_.py |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/compatibility/jsonschema/checks.py       |      370 |       46 |      164 |       21 |     84% |202, 252, 265, 334, 425, 435-446, 521-533, 546-550, 575, 599, 661, 674, 684, 764-776, 787-797, 822-825, 879->877, 881, 885, 895-900, 920 |
| src/karapace/compatibility/jsonschema/types.py        |      110 |        0 |       10 |        0 |    100% |           |
| src/karapace/compatibility/jsonschema/utils.py        |      132 |       22 |       64 |       11 |     80% |32, 46, 51, 105, 127-137, 150, 199-203, 209, 311->310, 330, 339, 355, 367 |
| src/karapace/compatibility/protobuf/\_\_init\_\_.py   |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/compatibility/protobuf/checks.py         |       17 |       13 |        6 |        0 |     17% |     11-25 |
| src/karapace/config.py                                |      199 |       64 |       50 |        5 |     60% |173-181, 233-247, 256, 263-265, 272-274, 279, 291-292, 299-325, 335-354 |
| src/karapace/constants.py                             |        8 |        0 |        0 |        0 |    100% |           |
| src/karapace/coordinator/\_\_init\_\_.py              |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/coordinator/master\_coordinator.py       |       65 |        9 |       12 |        3 |     84% |52-54, 57-61, 107-109, 124->126, 126->exit |
| src/karapace/coordinator/schema\_coordinator.py       |      476 |       38 |      140 |        9 |     91% |221-230, 243-246, 266-267, 279-280, 496-501, 519-523, 541, 551-555, 588->exit, 593->596, 607->634, 742, 759, 840, 884-885, 892-893 |
| src/karapace/dataclasses.py                           |       11 |        0 |        4 |        0 |    100% |           |
| src/karapace/dependency.py                            |       42 |       11 |       12 |        3 |     67% |18, 45, 49, 53-54, 58, 65, 68, 71-73 |
| src/karapace/errors.py                                |       40 |        5 |        2 |        1 |     86% |11-12, 65-67 |
| src/karapace/in\_memory\_database.py                  |      260 |      106 |      150 |        6 |     57% |31, 41, 45, 58, 62, 66, 70, 74, 78, 82, 86, 90, 94, 98, 102, 106, 110, 114, 118, 122, 126, 130, 134, 138, 159-172, 175-178, 181-186, 195-198, 209, 212, 218, 241, 270-272, 275->exit, 279-280, 293, 296->298, 302-310, 313-321, 327-330, 339-340, 347-351, 354-356, 359-360, 381-386, 389-390, 393-397 |
| src/karapace/instrumentation/\_\_init\_\_.py          |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/instrumentation/prometheus.py            |       38 |        0 |       10 |        0 |    100% |           |
| src/karapace/kafka/\_\_init\_\_.py                    |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/kafka/admin.py                           |       75 |        0 |       13 |        0 |    100% |           |
| src/karapace/kafka/common.py                          |       86 |        5 |       17 |        4 |     91% |52, 54, 75, 167, 205 |
| src/karapace/kafka/consumer.py                        |      141 |       15 |       26 |        3 |     89% |48-49, 62, 67, 99, 102-103, 134-135, 161-162, 181-182, 187-188 |
| src/karapace/kafka/producer.py                        |       67 |        2 |        6 |        0 |     97% |     63-64 |
| src/karapace/kafka/types.py                           |        8 |        0 |        0 |        0 |    100% |           |
| src/karapace/kafka\_error\_handler.py                 |       28 |        0 |        6 |        0 |    100% |           |
| src/karapace/kafka\_rest\_apis/\_\_init\_\_.py        |      630 |       69 |      188 |       18 |     88% |98-101, 301-313, 317-319, 480, 484, 491, 494, 505, 510->509, 533-543, 686, 729-730, 741->exit, 755-761, 801, 820, 843, 854, 879-880, 903, 972-973, 986-987, 1007, 1052-1057, 1101, 1160, 1210-1212, 1216-1217, 1221-1223, 1225-1227, 1234-1235, 1250, 1291, 1295->1301 |
| src/karapace/kafka\_rest\_apis/authentication.py      |       64 |        0 |       18 |        0 |    100% |           |
| src/karapace/kafka\_rest\_apis/consumer\_manager.py   |      344 |       38 |      118 |        9 |     88% |112, 195-196, 204->exit, 233-239, 249-250, 282-283, 298-301, 306, 324, 342, 438, 440, 475-476, 479, 481, 509-519, 540-541, 574, 588-589 |
| src/karapace/kafka\_rest\_apis/error\_codes.py        |       19 |        0 |        0 |        0 |    100% |           |
| src/karapace/kafka\_rest\_apis/schema\_cache.py       |       72 |       20 |       12 |        1 |     75% |18, 22, 26, 30, 34, 46, 53-54, 57-58, 72-73, 78, 86, 89-90, 98, 101, 104, 107 |
| src/karapace/kafka\_utils.py                          |       20 |        0 |        4 |        0 |    100% |           |
| src/karapace/karapace.py                              |       49 |        5 |       14 |        1 |     90% |51, 67, 81, 91, 99 |
| src/karapace/karapace\_all.py                         |       55 |        9 |       16 |        5 |     80% |36-37, 43-44, 46-47, 52-53, 71, 74->exit |
| src/karapace/key\_format.py                           |       31 |        0 |        6 |        0 |    100% |           |
| src/karapace/messaging.py                             |       57 |       24 |       10 |        1 |     51% |54-56, 60->exit, 64-100, 107-111 |
| src/karapace/offset\_watcher.py                       |       14 |        1 |        6 |        0 |     95% |        24 |
| src/karapace/protobuf/\_\_init\_\_.py                 |        0 |        0 |        0 |        0 |    100% |           |
| src/karapace/protobuf/compare\_result.py              |       61 |        1 |       10 |        0 |     99% |        66 |
| src/karapace/protobuf/compare\_type\_lists.py         |       42 |        8 |       22 |        4 |     75% |44, 48, 50-55, 63 |
| src/karapace/protobuf/compare\_type\_storage.py       |      111 |       19 |       48 |       12 |     78% |13-14, 24, 35, 61-63, 95, 99, 102, 107, 111, 114, 118, 126, 139-141, 144 |
| src/karapace/protobuf/encoding\_variants.py           |       44 |       30 |       18 |        2 |     29% |17-33, 37-45, 50, 56-67 |
| src/karapace/protobuf/enum\_constant\_element.py      |       21 |        0 |        4 |        0 |    100% |           |
| src/karapace/protobuf/enum\_element.py                |       51 |        5 |       26 |        4 |     88% |59-60, 73, 75, 78 |
| src/karapace/protobuf/exception.py                    |       25 |        9 |        6 |        1 |     55% |12, 44, 49-55 |
| src/karapace/protobuf/extend\_element.py              |       22 |        0 |        6 |        0 |    100% |           |
| src/karapace/protobuf/extensions\_element.py          |       26 |        1 |       10 |        2 |     92% |28->38, 36 |
| src/karapace/protobuf/field.py                        |        7 |        0 |        0 |        0 |    100% |           |
| src/karapace/protobuf/field\_element.py               |      101 |        8 |       40 |        6 |     90% |110, 113, 143, 149, 160, 167, 172, 175 |
| src/karapace/protobuf/group\_element.py               |       27 |        0 |        8 |        1 |     97% |    33->37 |
| src/karapace/protobuf/io.py                           |      179 |       87 |       50 |        8 |     46% |34, 38-55, 78-83, 88, 91, 99-149, 158-168, 181-186, 214-217, 238->240, 253-267, 297-298, 310->316, 311->310, 317 |
| src/karapace/protobuf/known\_dependency.py            |       21 |        0 |       10 |        1 |     97% |    14->16 |
| src/karapace/protobuf/kotlin\_wrapper.py              |       21 |        1 |       10 |        0 |     97% |        36 |
| src/karapace/protobuf/location.py                     |       27 |        3 |        8 |        3 |     83% |19, 30, 39, 46->50 |
| src/karapace/protobuf/message\_element.py             |      117 |        3 |       62 |        3 |     97% |97-98, 100->exit, 168 |
| src/karapace/protobuf/one\_of\_element.py             |       49 |        0 |       22 |        1 |     99% |    44->48 |
| src/karapace/protobuf/option\_element.py              |       80 |        2 |       32 |        3 |     96% |50, 80->83, 87 |
| src/karapace/protobuf/option\_reader.py               |      104 |       17 |       50 |        4 |     81% |60, 100, 111-115, 121-127, 135-139 |
| src/karapace/protobuf/proto\_file\_element.py         |       94 |        4 |       46 |        3 |     95% |20->exit, 32, 118, 128, 143 |
| src/karapace/protobuf/proto\_normalizations.py        |      116 |        4 |       12 |        2 |     95% |151-154, 211, 222 |
| src/karapace/protobuf/proto\_parser.py                |      383 |        9 |      138 |       10 |     96% |109, 117, 295, 321, 349, 486, 514, 516->519, 553, 616 |
| src/karapace/protobuf/proto\_type.py                  |      130 |       19 |       42 |        5 |     83% |17->19, 122, 146-147, 157, 160-169, 172, 178, 181, 185, 197, 202, 210, 249 |
| src/karapace/protobuf/protobuf\_to\_dict.py           |      186 |      159 |      108 |        0 |      9% |25-28, 32-33, 63, 67-69, 73, 86-133, 144-161, 189-193, 197-227, 231-305, 309-316, 323-332, 346-351 |
| src/karapace/protobuf/protopace/\_\_init\_\_.py       |        1 |        0 |        0 |        0 |    100% |           |
| src/karapace/protobuf/protopace/protopace.py          |       88 |       39 |       20 |        4 |     55% |17, 51, 70-73, 81-104, 154-158, 162-171, 175-184, 188-189 |
| src/karapace/protobuf/reserved\_element.py            |       27 |        1 |       12 |        1 |     95% |        38 |
| src/karapace/protobuf/rpc\_element.py                 |       30 |        0 |        8 |        0 |    100% |           |
| src/karapace/protobuf/schema.py                       |      278 |       14 |      156 |        8 |     94% |76-78, 81-83, 178, 200, 205, 278, 290, 301, 448, 505 |
| src/karapace/protobuf/serialization.py                |      254 |       35 |      172 |       40 |     82% |58, 60, 116, 120, 122, 124, 126, 128, 132, 134, 136, 142, 144, 148, 199, 208, 215->217, 217->219, 243, 256->245, 279, 281, 283, 285, 287, 289, 291, 293, 295, 297, 299, 301, 303, 305, 307, 309, 311, 313, 322->325, 334->331 |
| src/karapace/protobuf/service\_element.py             |       28 |        0 |       10 |        0 |    100% |           |
| src/karapace/protobuf/syntax.py                       |       12 |        1 |        2 |        0 |     93% |        24 |
| src/karapace/protobuf/syntax\_reader.py               |      262 |       13 |      124 |       15 |     92% |53-54, 66, 105, 168->179, 191-192, 214, 218->221, 225->246, 247, 248->259, 252->258, 260, 265->274, 300->323, 313-314, 345, 368 |
| src/karapace/protobuf/type\_element.py                |       25 |        9 |        4 |        1 |     66% |14-16, 32, 35-36, 39-40, 43 |
| src/karapace/protobuf/type\_tree.py                   |       65 |        2 |       36 |        3 |     95% |66, 70->68, 73 |
| src/karapace/protobuf/utils.py                        |       46 |        3 |       20 |        3 |     91% |10, 20, 52 |
| src/karapace/rapu.py                                  |      262 |       55 |       96 |       19 |     75% |84, 88, 103, 107, 140, 143, 234, 243-252, 277-281, 286, 292-301, 307, 314, 317->320, 322->325, 325->328, 335, 341-342, 347-354, 359-364, 371, 374-375, 383, 405-408, 451, 467, 481-483 |
| src/karapace/schema\_models.py                        |      177 |       34 |       64 |       10 |     81% |85-86, 93->98, 96, 128, 135-136, 152-154, 159-166, 173, 185-194, 208, 218-219, 225-226, 238-250, 336, 342-343, 346, 349-351, 376, 430->exit |
| src/karapace/schema\_reader.py                        |      407 |      102 |      132 |       21 |     71% |186-196, 204-214, 233-241, 251-252, 269, 272-276, 281, 287-299, 324, 333, 349->352, 378-386, 400->409, 483->486, 490-491, 492->exit, 498-499, 502-507, 513, 517-521, 525-526, 539-541, 555-557, 558->578, 570, 586-587, 599-600, 613, 630, 637, 640-643, 662-674, 684, 686-688 |
| src/karapace/schema\_references.py                    |       36 |        4 |        8 |        1 |     89% |26, 40, 57, 60 |
| src/karapace/schema\_registry.py                      |      245 |      182 |       94 |        0 |     20% |64, 68, 71-72, 92-101, 104-112, 115-117, 120-128, 131-190, 193-227, 230-237, 240-266, 271-282, 285-286, 303-401, 406-414, 417, 420, 432-447, 450-452, 455-456, 462, 465-467 |
| src/karapace/schema\_registry\_apis.py                |      540 |      400 |      187 |        7 |     22% |90-91, 102-118, 130, 133-135, 139, 145, 148, 335->337, 338, 341-361, 365, 382-456, 459-479, 488-552, 557-581, 584, 587-591, 594-619, 624-650, 667-692, 702-717, 720-729, 734-787, 792-819, 824-894, 899-916, 926-951, 956-963, 974-975, 986, 1006-1027, 1030-1031, 1046-1080, 1085-1191, 1208-1291, 1299-1300, 1313-1325, 1332-1335, 1340-1357, 1360 |
| src/karapace/schema\_type.py                          |        6 |        0 |        2 |        0 |    100% |           |
| src/karapace/sentry/\_\_init\_\_.py                   |       16 |        3 |        0 |        0 |     81% | 11, 25-26 |
| src/karapace/sentry/sentry\_client.py                 |       21 |        0 |        2 |        1 |     96% |  58->exit |
| src/karapace/sentry/sentry\_client\_api.py            |       13 |        2 |        0 |        0 |     85% |    23, 26 |
| src/karapace/serialization.py                         |      275 |       28 |      106 |       14 |     89% |83, 121, 138, 149, 153, 179-180, 210, 224-225, 232, 246-247, 289-290, 339->342, 373, 378, 449-454, 460-463, 481, 490 |
| src/karapace/statsd.py                                |       60 |        9 |       18 |        4 |     81% |35-37, 61, 70, 72-74, 76 |
| src/karapace/typing.py                                |       69 |        2 |       22 |        2 |     96% |    75, 87 |
| src/karapace/utils.py                                 |      134 |       44 |       54 |        3 |     68% |31-33, 45-47, 52, 57, 62, 67, 73-82, 93, 105, 118, 132, 137, 152, 156, 198, 206-209, 228-246 |
| src/karapace/version.py                               |       11 |        2 |        2 |        1 |     77% |       5-6 |
|                                             **TOTAL** | **10702** | **2146** | **3868** |  **380** | **77%** |           |


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