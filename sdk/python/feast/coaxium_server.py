import pickle
from concurrent import futures
from typing import TYPE_CHECKING

import grpc

import feast.protos.feast.serving.FeatureStore_pb2 as pb2
import feast.protos.feast.serving.FeatureStore_pb2_grpc as pb2_grpc

if TYPE_CHECKING:
    from feast import FeatureStore


class FeatureStoreService(pb2_grpc.FeatureStoreServicer):
    def __init__(self, store: "FeatureStore", *args, **kwargs):

        self.FeatureStore = store

    def GetHistoricalFeatures(self, request, context):

        pit_df = self.FeatureStore.get_historical_features(
            api_key=request.key,
            features=pickle.loads(request.feature_list),
            entity_df=pickle.loads(request.entity_df),
        ).to_df()

        result = {"pit_df": pickle.dumps(pit_df)}

        return pb2.GHFResponse(**result)

    def GetOnlineFeatures(self, request, context):

        online_dict = self.FeatureStore.get_online_features(
            api_key=request.key,
            features=pickle.loads(request.feature_list),
            entity_rows=pickle.loads(request.entity_list),
        ).to_dict()

        result = {"online_dict": pickle.dumps(online_dict)}

        return pb2.GOFResponse(**result)


def start_server(store: "FeatureStore", *args, **kwargs):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_FeatureStoreServicer_to_server(FeatureStoreService(store), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()
