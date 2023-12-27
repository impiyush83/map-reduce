import pytest
import map_reduce
import master


def test_success_init_cluster():
    mr_lib = map_reduce.MapReduceLibrary()
    response = mr_lib.init_cluster(3, 3)
    assert response.status_code == 200
