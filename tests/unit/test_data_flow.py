# from unittest.mock import patch, MagicMock #https://github.com/python/cpython/issues/100950

# import pytest
# from pytest_cases import case, parametrize_with_cases

# from core.data_flow import (
#     DataFlow,
#     DataFlowManager,
#     DirectDataFlow,
#     PersistentDataFlow,
#     QueueDataFlow
# )

# class ParseCases:

#     @staticmethod
#     @case(tags="success")
#     def case_parse_queue_data_flow() -> tuple[str, DataFlow]:
#         return "queue", QueueDataFlow

#     @staticmethod
#     @case(tags="success")
#     def case_parse_direct_data_flow() -> tuple[str, DataFlow]:
#         return "direct", DirectDataFlow

#     @staticmethod
#     @case(tags="success")
#     def case_parse_persistent_data_flow() -> tuple[str, DataFlow]:
#         return "persistent", PersistentDataFlow

# @parametrize_with_cases("data_flow_strategy, class_type", cases=ParseCases, has_tag="success")
# def test_get_data_flow_success(data_flow_strategy, class_type) -> None:
#     result = DataFlowManager._get_data_flow(data_flow_strategy)  # pylint: disable=protected-access

#     assert result == class_type


# def test_get_data_flow_failure() -> None:
#     with pytest.raises(ValueError):
#         DataFlowManager._get_data_flow("unknown_data_flow")


# class DataFlowCases:
    
#     @case(tags="direct_flow_success")
#     def case_transfer_direct_flow() -> tuple[str, DataFlow]:
#         return "extracted data", DirectDataFlow


# @patch.object(DataFlowManager, "_get_data_flow")
# def test_execute_flow_sucess(flow_mock: MagicMock):
#     flow_mock.return_value = QueueDataFlow

#     print(DataFlowManager.execute_flow(data, "queue"))