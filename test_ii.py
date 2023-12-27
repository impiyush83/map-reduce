import pytest
import map_reduce
import master


def test_success_init_cluster():
    mr_lib = map_reduce.MapReduceLibrary()
    response = mr_lib.init_cluster(3, 3)
    assert response.status_code == 200


def test_success_inverted_index_map_reduce():
    mr_lib = map_reduce.MapReduceLibrary()
    response = mr_lib.init_cluster(3, 3)
    assert response.status_code == 200
    response = mr_lib.run_mapred(
        'books',
        'inverted_index',
        'inverted_index',
        'inverted_index/results.txt'
    )
    assert response.status_code == 200


