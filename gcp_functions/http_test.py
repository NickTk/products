
from unittest.mock import Mock

import main

def check_cosine():
    data = {"id":540992, "cnd": [19769, 538131, 446072, 540018]}
    req = Mock(get_json=Mock(return_value=data), args=data)
    
    assert len(main.check_similar_goods(req)) > 0
