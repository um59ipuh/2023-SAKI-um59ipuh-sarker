# TestETL_services.py
import ETL_services as service

def test_get_state_by_coord():
    lat_lon = "49.652969, 8.554764"
    actual_state = "Hessen"
    assert service.get_state_by_coord(lat_lon) == actual_state