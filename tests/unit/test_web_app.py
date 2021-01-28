from karapace.rapu import HTTPRequest


async def test_header_get():
    req = HTTPRequest(url="", query="", headers={}, path_for_stats="", method="GET")
    assert "Content-Type" not in req.headers
    for v in ["Content-Type", "content-type", "CONTENT-tYPE", "coNTENT-TYpe"]:
        assert req.get_header(v) == "application/json"
