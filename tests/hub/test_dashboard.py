def test_root_serves_html(client):
    """GET / → index.html 반환."""
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
