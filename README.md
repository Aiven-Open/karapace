# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/Aiven-Open/karapace/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                     |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|--------------------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| src/karapace/\_\_main\_\_.py                             |       36 |       36 |        2 |        0 |      0% |     6-100 |
| src/karapace/api/container.py                            |       12 |        0 |        0 |        0 |    100% |           |
| src/karapace/api/content\_type.py                        |       28 |       19 |        8 |        0 |     25% |     30-66 |
| src/karapace/api/controller.py                           |      350 |      261 |       74 |        8 |     23% |80, 83-101, 105, 124-149, 160-180, 195-196, 206-211, 214-216, 226, 236-237, 245, 247, 249, 251, 270-293, 296, 301, 308-320, 329-347, 361-373, 380-381, 390-398, 406-435, 452-482, 491-544, 552-566, 580-604, 612-617, 629, 653-688, 698-786, 803-873, 878, 885-893, 896-899, 902-913, 922-945 |
| src/karapace/api/factory.py                              |       42 |       18 |        0 |        0 |     57% |38-47, 57-69 |
| src/karapace/api/forward\_client.py                      |       47 |        3 |       14 |        5 |     87% |48->50, 57-58, 67->exit, 76->exit, 98 |
| src/karapace/api/http\_handlers/\_\_init\_\_.py          |       19 |       11 |        2 |        0 |     38% |     16-28 |
| src/karapace/api/middlewares/\_\_init\_\_.py             |       21 |       15 |        2 |        0 |     26% |     14-37 |
| src/karapace/api/routers/compatibility.py                |       21 |        4 |        2 |        0 |     74% |     38-42 |
| src/karapace/api/routers/config.py                       |       64 |       26 |       22 |        1 |     50% |37-40, 55, 76-80, 95-104, 120-129 |
| src/karapace/api/routers/errors.py                       |       40 |        3 |        0 |        0 |     92% | 52-53, 67 |
| src/karapace/api/routers/health.py                       |       43 |       17 |        4 |        0 |     55% |41-49, 58-86 |
| src/karapace/api/routers/master\_availability.py         |       26 |        8 |        4 |        0 |     60% |     43-60 |
| src/karapace/api/routers/metrics.py                      |        9 |        1 |        0 |        0 |     89% |        23 |
| src/karapace/api/routers/mode.py                         |       27 |        7 |        4 |        0 |     65% |35-38, 49-53 |
| src/karapace/api/routers/raw\_path\_router.py            |       18 |       10 |        6 |        0 |     33% |     30-44 |
| src/karapace/api/routers/requests.py                     |       65 |        3 |        2 |        0 |     93% |     30-35 |
| src/karapace/api/routers/root.py                         |        5 |        1 |        0 |        0 |     80% |        16 |
| src/karapace/api/routers/schemas.py                      |       26 |        4 |        0 |        0 |     85% |34, 53, 80, 93 |
| src/karapace/api/routers/setup.py                        |       20 |        9 |        0 |        0 |     55% |     19-27 |
| src/karapace/api/routers/subjects.py                     |       88 |       43 |       24 |        0 |     40% |43, 61-65, 85-94, 111-117, 135-139, 152-156, 172-181, 195-199, 211-215 |
| src/karapace/api/telemetry/container.py                  |       18 |        1 |        0 |        0 |     94% |        18 |
| src/karapace/api/telemetry/metrics.py                    |       30 |        0 |        0 |        0 |    100% |           |
| src/karapace/api/telemetry/middleware.py                 |       30 |        0 |        0 |        0 |    100% |           |
| src/karapace/api/telemetry/setup.py                      |       20 |        2 |        0 |        0 |     90% |     36-37 |
| src/karapace/api/user.py                                 |       16 |        8 |        2 |        0 |     44% |     20-31 |
| src/karapace/backup/api.py                               |      303 |       18 |      100 |       13 |     91% |143, 153-155, 169, 173, 280-282, 393, 408->exit, 489, 496, 536, 538, 574-578, 678, 694 |
| src/karapace/backup/backends/reader.py                   |       48 |        0 |        4 |        1 |     98% |  82->exit |
| src/karapace/backup/backends/v1.py                       |       13 |        0 |        2 |        0 |    100% |           |
| src/karapace/backup/backends/v2.py                       |       56 |        2 |       14 |        4 |     91% |60, 62, 72->74, 77->79 |
| src/karapace/backup/backends/v3/backend.py               |      154 |       12 |       44 |        8 |     89% |49-53, 62-63, 125, 253, 272, 274, 281, 310, 313 |
| src/karapace/backup/backends/v3/checksum.py              |        5 |        0 |        4 |        2 |     78% |12->exit, 14->exit |
| src/karapace/backup/backends/v3/constants.py             |        2 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/v3/errors.py                |       27 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/v3/readers.py               |       47 |        0 |        8 |        0 |    100% |           |
| src/karapace/backup/backends/v3/schema.py                |       63 |        0 |        0 |        0 |    100% |           |
| src/karapace/backup/backends/v3/schema\_tool.py          |       79 |       79 |       22 |        0 |      0% |     8-146 |
| src/karapace/backup/backends/v3/writers.py               |       46 |        1 |        4 |        1 |     96% |        25 |
| src/karapace/backup/backends/writer.py                   |       46 |        0 |        2 |        1 |     98% | 169->exit |
| src/karapace/backup/cli.py                               |       85 |       10 |       18 |        2 |     86% |164-179, 188, 200-208, 210-216, 220 |
| src/karapace/backup/encoders.py                          |       20 |        0 |       12 |        0 |    100% |           |
| src/karapace/backup/errors.py                            |       41 |        7 |        2 |        0 |     84% |57, 62, 67, 72, 80, 85, 90 |
| src/karapace/backup/poll\_timeout.py                     |       30 |        0 |        4 |        0 |    100% |           |
| src/karapace/backup/safe\_writer.py                      |       68 |        0 |       14 |        0 |    100% |           |
| src/karapace/backup/topic\_configurations.py             |        9 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/anonymize\_schemas/anonymize\_avro.py  |       61 |        0 |       22 |        0 |    100% |           |
| src/karapace/core/auth.py                                |      203 |       79 |       46 |       10 |     59% |55-62, 73, 102->exit, 106->exit, 108->exit, 110->exit, 116->exit, 118->exit, 126, 130, 134, 138, 142, 146, 155-158, 175, 194, 208-212, 216, 221-244, 248-251, 254-284, 288-292, 300, 304-321, 325 |
| src/karapace/core/auth\_container.py                     |        8 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/avro\_dataclasses/introspect.py        |       86 |        7 |       50 |        7 |     90% |20, 41, 69, 115, 117, 123, 126 |
| src/karapace/core/avro\_dataclasses/models.py            |       99 |        6 |       34 |        4 |     92% |23, 105-106, 108, 111, 131 |
| src/karapace/core/avro\_dataclasses/schema.py            |       33 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/client.py                              |      133 |        4 |       18 |        1 |     97% |69-70, 80-81 |
| src/karapace/core/compatibility/\_\_init\_\_.py          |       15 |        2 |        0 |        0 |     87% |     37-42 |
| src/karapace/core/compatibility/jsonschema/checks.py     |      370 |       46 |      162 |       21 |     84% |203, 253, 266, 335, 426, 436-447, 522-534, 547-551, 576, 600, 662, 675, 685, 765-777, 788-798, 823-826, 880->878, 882, 886, 896-901, 921 |
| src/karapace/core/compatibility/jsonschema/types.py      |      111 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/compatibility/jsonschema/utils.py      |      132 |       22 |       62 |       11 |     80% |33, 47, 52, 106, 128-138, 151, 200-204, 210, 312->311, 331, 340, 356, 368 |
| src/karapace/core/compatibility/protobuf/checks.py       |       17 |       13 |        6 |        0 |     17% |     12-26 |
| src/karapace/core/compatibility/schema\_compatibility.py |       61 |       25 |       28 |        6 |     49% |42, 49-66, 77, 82, 86->127, 98-125, 131, 139 |
| src/karapace/core/config.py                              |      234 |       81 |       56 |        5 |     57% |32-33, 141, 144, 147, 150, 179-183, 193->197, 197->203, 213, 214->218, 238-246, 274-279, 286-288, 295-297, 302, 308, 312, 316, 321-347, 351-376 |
| src/karapace/core/constants.py                           |        8 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/container.py                           |        8 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/coordinator/master\_coordinator.py     |       99 |       15 |       18 |        6 |     80% |83-85, 88-92, 102->104, 107, 109-110, 153-158, 183, 193 |
| src/karapace/core/coordinator/schema\_coordinator.py     |      514 |       40 |      140 |       10 |     91% |199, 277-286, 299-302, 322-323, 335-336, 500, 586-591, 611-615, 633, 643-647, 680->exit, 685->688, 699->726, 834, 851, 932, 976-977, 984-985 |
| src/karapace/core/dataclasses.py                         |        8 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/dependency.py                          |       42 |       11 |       10 |        3 |     65% |18, 45, 49, 53-54, 58, 65, 68, 71-73 |
| src/karapace/core/errors.py                              |       40 |        5 |        2 |        1 |     86% |11-12, 65-67 |
| src/karapace/core/in\_memory\_database.py                |      264 |       69 |       84 |       11 |     70% |32, 42, 46, 59, 63, 67, 71, 75, 79, 83, 87, 91, 95, 99, 103, 107, 111, 115, 119, 123, 127, 131, 152-165, 170, 177, 188-191, 211, 234, 266-268, 271->exit, 275-276, 289, 292->294, 298-306, 323-326, 343-347, 352->351, 360->exit, 361->363, 387, 400 |
| src/karapace/core/instrumentation/meter.py               |       33 |        3 |        6 |        1 |     90% |35, 38, 41, 57->exit |
| src/karapace/core/instrumentation/prometheus.py          |       40 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/instrumentation/tracer.py              |       65 |        2 |       12 |        3 |     94% |41, 44, 60->exit, 88->exit, 102->exit |
| src/karapace/core/kafka/admin.py                         |       82 |        2 |       12 |        0 |     98% |   191-194 |
| src/karapace/core/kafka/common.py                        |       87 |        5 |       20 |        6 |     90% |59, 61, 63, 83->exit, 175, 211 |
| src/karapace/core/kafka/consumer.py                      |      142 |       15 |       20 |        3 |     89% |49-50, 63, 68, 100, 103-104, 135-136, 162-163, 182-183, 188-189 |
| src/karapace/core/kafka/producer.py                      |       67 |        2 |        6 |        0 |     97% |     68-69 |
| src/karapace/core/kafka/types.py                         |        8 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/kafka\_error\_handler.py               |       28 |        0 |        6 |        0 |    100% |           |
| src/karapace/core/kafka\_utils.py                        |       20 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/key\_format.py                         |       36 |        0 |        6 |        0 |    100% |           |
| src/karapace/core/logging\_setup.py                      |       31 |        9 |       12 |        0 |     65% | 22, 38-45 |
| src/karapace/core/messaging.py                           |       57 |       31 |       10 |        0 |     39% |38-57, 60-62, 65-101, 108-112 |
| src/karapace/core/metrics\_container.py                  |        8 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/offset\_watcher.py                     |       17 |        2 |        0 |        0 |     88% |     27-30 |
| src/karapace/core/protobuf/compare\_result.py            |       60 |        1 |        6 |        0 |     98% |        66 |
| src/karapace/core/protobuf/compare\_type\_lists.py       |       42 |        8 |       22 |        4 |     75% |45, 49, 51-56, 64 |
| src/karapace/core/protobuf/compare\_type\_storage.py     |      111 |       19 |       42 |       12 |     80% |14-15, 25, 36, 62-64, 96, 100, 103, 108, 112, 115, 119, 127, 140-142, 145 |
| src/karapace/core/protobuf/encoding\_variants.py         |       43 |       30 |       18 |        2 |     28% |16-32, 36-44, 49, 55-66 |
| src/karapace/core/protobuf/enum\_constant\_element.py    |       21 |        0 |        2 |        0 |    100% |           |
| src/karapace/core/protobuf/enum\_element.py              |       51 |        5 |       26 |        5 |     87% |56->59, 60-61, 74, 76, 79 |
| src/karapace/core/protobuf/exception.py                  |       25 |        9 |        6 |        1 |     55% |13, 45, 50-56 |
| src/karapace/core/protobuf/extend\_element.py            |       22 |        0 |        4 |        0 |    100% |           |
| src/karapace/core/protobuf/extensions\_element.py        |       26 |        1 |        8 |        2 |     91% |28->38, 36 |
| src/karapace/core/protobuf/field.py                      |        7 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/protobuf/field\_element.py             |      101 |        8 |       38 |        6 |     90% |110, 113, 143, 149, 160, 167, 172, 175 |
| src/karapace/core/protobuf/group\_element.py             |       27 |        0 |        6 |        1 |     97% |    34->38 |
| src/karapace/core/protobuf/io.py                         |      178 |       85 |       48 |       10 |     48% |40, 44-60, 86-91, 95->exit, 97->exit, 105-155, 164-174, 187-192, 220-223, 244->246, 259-273, 303-304, 316->322, 317->316, 323 |
| src/karapace/core/protobuf/known\_dependency.py          |       21 |        0 |        6 |        1 |     96% |    14->16 |
| src/karapace/core/protobuf/kotlin\_wrapper.py            |       21 |        1 |        8 |        0 |     97% |        37 |
| src/karapace/core/protobuf/location.py                   |       27 |        3 |        8 |        3 |     83% |19, 30, 39, 46->50 |
| src/karapace/core/protobuf/message\_element.py           |      117 |        3 |       62 |        3 |     97% |98-99, 101->exit, 169 |
| src/karapace/core/protobuf/one\_of\_element.py           |       49 |        0 |       22 |        1 |     99% |    44->48 |
| src/karapace/core/protobuf/option\_element.py            |       80 |        2 |       30 |        3 |     95% |51, 81->84, 88 |
| src/karapace/core/protobuf/option\_reader.py             |      103 |       17 |       46 |        4 |     81% |60, 100, 111-115, 121-127, 135-139 |
| src/karapace/core/protobuf/proto\_file\_element.py       |       95 |        4 |       44 |        3 |     95% |23->exit, 35, 121, 131, 146 |
| src/karapace/core/protobuf/proto\_normalizations.py      |      115 |        4 |       10 |        2 |     95% |150-153, 210, 221 |
| src/karapace/core/protobuf/proto\_parser.py              |      382 |        9 |      130 |       10 |     96% |108, 116, 293, 319, 347, 484, 512, 514->517, 551, 614 |
| src/karapace/core/protobuf/proto\_type.py                |      130 |       19 |       26 |        5 |     81% |18->20, 123, 147-148, 158, 161-170, 173, 179, 182, 186, 198, 203, 211, 250 |
| src/karapace/core/protobuf/protobuf\_to\_dict.py         |      185 |      159 |      102 |        0 |      9% |25-28, 32-33, 61, 65-67, 71, 84-131, 142-159, 187-191, 195-223, 227-301, 305-311, 318-327, 341-346 |
| src/karapace/core/protobuf/protopace/\_\_init\_\_.py     |        1 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/protobuf/protopace/protopace.py        |       87 |       39 |       16 |        4 |     52% |16, 50, 69-72, 80-103, 153-157, 161-170, 174-183, 187-188 |
| src/karapace/core/protobuf/reserved\_element.py          |       27 |        1 |       10 |        1 |     95% |        39 |
| src/karapace/core/protobuf/rpc\_element.py               |       30 |        0 |        8 |        0 |    100% |           |
| src/karapace/core/protobuf/schema.py                     |      278 |       14 |      150 |        8 |     94% |76-78, 81-83, 178, 200, 205, 278, 290, 301, 448, 505 |
| src/karapace/core/protobuf/serialization.py              |      261 |       34 |      172 |       39 |     83% |60, 62, 132, 136, 138, 140, 142, 144, 148, 150, 152, 158, 160, 164, 215, 231->233, 233->235, 259, 272->261, 295, 297, 299, 301, 303, 305, 307, 309, 311, 313, 315, 317, 319, 321, 323, 325, 327, 329, 338->341, 350->347 |
| src/karapace/core/protobuf/service\_element.py           |       28 |        0 |        8 |        0 |    100% |           |
| src/karapace/core/protobuf/syntax.py                     |       12 |        1 |        0 |        0 |     92% |        24 |
| src/karapace/core/protobuf/syntax\_reader.py             |      262 |       13 |      124 |       15 |     92% |55-56, 68, 107, 170->181, 193-194, 216, 220->223, 227->247, 248, 249->260, 253->259, 261, 266->275, 301->324, 314-315, 346, 369 |
| src/karapace/core/protobuf/type\_element.py              |       26 |        9 |        2 |        1 |     64% |16-18, 34, 37-38, 41-42, 45 |
| src/karapace/core/protobuf/type\_tree.py                 |       64 |        2 |       16 |        3 |     94% |66, 70->68, 73 |
| src/karapace/core/protobuf/utils.py                      |       46 |        3 |       20 |        3 |     91% |11, 21, 53 |
| src/karapace/core/schema\_models.py                      |      178 |       32 |       40 |        9 |     80% |87-88, 95->100, 98, 130, 154-156, 161-168, 175, 187-196, 210, 220-221, 227-228, 240-252, 338, 344-345, 348, 351-353, 378, 432->exit |
| src/karapace/core/schema\_reader.py                      |      435 |      105 |       92 |       15 |     75% |198-208, 216-226, 245-253, 264-265, 267-268, 272->274, 317, 320-324, 336-348, 373-376, 384-385, 394, 429, 437-445, 458->467, 531->534, 538-539, 540->exit, 546-547, 550-555, 561, 565-569, 573-574, 587-589, 603-605, 606->626, 618, 634-635, 657-658, 674, 677-680, 699-711, 721, 723-725 |
| src/karapace/core/schema\_references.py                  |       37 |        3 |        2 |        1 |     90% |27, 41, 61 |
| src/karapace/core/schema\_registry.py                    |      259 |      194 |       78 |        0 |     19% |80, 84, 87-88, 91-93, 96-99, 106-116, 119-127, 130-132, 135-143, 146-203, 206-238, 241-248, 251-277, 282-293, 296-297, 314-393, 398-406, 409, 412, 424-439, 442-444, 447-448, 454, 457-459, 466-495, 499-504 |
| src/karapace/core/schema\_type.py                        |        6 |        0 |        0 |        0 |    100% |           |
| src/karapace/core/sentry/\_\_init\_\_.py                 |       16 |        3 |        0 |        0 |     81% | 11, 25-26 |
| src/karapace/core/sentry/sentry\_client.py               |       23 |        2 |        2 |        1 |     88% |56-57, 61->exit |
| src/karapace/core/sentry/sentry\_client\_api.py          |       13 |        2 |        0 |        0 |     85% |    24, 27 |
| src/karapace/core/serialization.py                       |      277 |       29 |       88 |       13 |     88% |93, 131, 148, 159, 163, 189-190, 220, 234-235, 242, 256-257, 299-300, 349->352, 383, 388-389, 459-464, 470-473, 491, 500 |
| src/karapace/core/stats.py                               |       42 |       12 |        0 |        0 |     71% |62-65, 70, 73, 76-77, 80-88, 91 |
| src/karapace/core/typing.py                              |       91 |        7 |       10 |        2 |     89% |48-50, 93, 105, 127, 131 |
| src/karapace/core/utils.py                               |      110 |       34 |       42 |       12 |     61% |31-33, 45-47, 51->exit, 55->exit, 59->exit, 63->exit, 69-78, 82->exit, 92->exit, 112, 125->exit, 129->exit, 140, 144, 148, 190, 205-223 |
| src/karapace/kafka\_rest\_apis/\_\_init\_\_.py           |      636 |       74 |      142 |       15 |     88% |101-104, 304-323, 327-329, 490, 494, 501, 504, 515, 520->557, 543-553, 696, 739-740, 751->exit, 765-771, 811, 830, 853, 864, 889-890, 913, 982-983, 996-997, 1017, 1062-1067, 1111, 1170, 1220-1222, 1226-1227, 1231-1233, 1235-1237, 1244-1245, 1260, 1301, 1311 |
| src/karapace/kafka\_rest\_apis/\_\_main\_\_.py           |       32 |       32 |        2 |        0 |      0% |      6-48 |
| src/karapace/kafka\_rest\_apis/authentication.py         |       64 |        0 |       14 |        0 |    100% |           |
| src/karapace/kafka\_rest\_apis/consumer\_manager.py      |      351 |       38 |       72 |        8 |     89% |113, 206-207, 215->exit, 245-251, 261-262, 294-295, 310-313, 318, 340, 358, 454, 456, 489-490, 493, 495, 523-533, 554-555, 588, 602-603 |
| src/karapace/kafka\_rest\_apis/convert\_to\_int.py       |        9 |        3 |        2 |        0 |     73% |     16-19 |
| src/karapace/kafka\_rest\_apis/error\_codes.py           |       19 |        0 |        0 |        0 |    100% |           |
| src/karapace/kafka\_rest\_apis/karapace.py               |       58 |        8 |        4 |        1 |     82% |59, 75, 89, 99, 108-111 |
| src/karapace/kafka\_rest\_apis/schema\_cache.py          |       73 |       20 |        2 |        1 |     72% |19, 23, 27, 31, 35, 47, 54-55, 58-59, 73-74, 79, 87, 90-91, 99, 102, 105, 108 |
| src/karapace/rapu.py                                     |      261 |       67 |       80 |       20 |     70% |85->exit, 88->exit, 103, 107, 140, 143, 229-252, 277-281, 286, 292-297, 301, 308, 311->314, 314->324, 316->319, 319->322, 325-326, 329, 333, 335-336, 341-348, 353-358, 365, 368-369, 377, 399-402, 438, 445, 461, 475-477, 480-482 |
| src/karapace/statsd.py                                   |       61 |       30 |       16 |        1 |     42% |37-39, 42, 51-58, 65-87 |
| src/karapace/version.py                                  |       13 |        3 |        2 |        1 |     73% |      8-11 |
|                                                **TOTAL** | **11755** | **2336** | **3210** |  **407** | **77%** |           |

16 empty files skipped.


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