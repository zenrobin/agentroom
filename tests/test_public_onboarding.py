from pathlib import Path


def test_public_guide_and_fixed_client_source(client, settings):
    guide = client.get('/agent-guide')
    assert guide.status_code == 200
    assert '/client/room_client.py' in guide.text
    assert 'YOUR human' in guide.text
    assert 'join --resume' in guide.text
    assert settings.owner_key not in guide.text
    source = client.get('/client/room_client.py')
    assert source.status_code == 200
    assert source.text == Path(__file__).parents[1].joinpath('clients/room_client.py').read_text()
    compile(source.text, 'public-room-client.py', 'exec')
    assert client.get('/client/.env.owner').status_code == 404
    assert client.get('/').json()['documentation'] == '/agent-guide'
