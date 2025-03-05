from datetime import datetime, timedelta
import pprint
from flask import json, jsonify, request
from flask_cors import cross_origin
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask_restx import Namespace, Resource, reqparse
import numpy as np

from miniagro.app_data.app_daily_summary import AppDailySummaryBuilder, RangeSummaryRecordBuilder
from miniagro.app_data.app_dynamic_nodes import AppDynamicNodeBuilder
from miniagro.app_data.app_nodes import AppNodeBuilder
from miniagro.app_data.app_report_views import ReportView
from miniagro.config.user_config import UserConfig
from miniagro.data_utils.sensor_utils import SensorUtils
from miniagro.data_utils.source_record_utils import SourceRecordUtils
from miniagro.utils.api_utils import APIUtils
from miniagro.utils.param_utils import DictUtils


ns_mini_agro = Namespace('mini_agro', description='Mini Agro related operations', decorators=[cross_origin()])




@ns_mini_agro.route('/get_app_nodes/')
class GetAppNodes(Resource):
    @ns_mini_agro.doc('get_app_nodes')
    @cross_origin()
    @jwt_required()
    def get(self):
        user_id = get_jwt_identity()
        source_ids = APIUtils.filter_user_source_ids(user_id)
        if not source_ids:
            return jsonify({'error': 'No sources found'}), 400
        builder = AppNodeBuilder()
        builder.build_nodes(source_ids)
        js = json.dumps(builder.to_api_dict(flat=True), default=str)

        return jsonify(json.loads(js)), 200


@ns_mini_agro.route('/get_app_nodes_mini/')
class GetAppNodesMini(Resource):
    @ns_mini_agro.doc('get_app_nodes_mini')
    @cross_origin()
    @jwt_required()
    def get(self):
        user_id = get_jwt_identity()
        source_ids = APIUtils.filter_user_source_ids(user_id)
        if not source_ids:
            return jsonify({'error': 'No sources found'}), 400
        builder = AppNodeBuilder()
        builder.build_nodes(source_ids)
        js = json.dumps(builder.to_mini_api_dict(flat=True), default=str)
        return jsonify(json.loads(js)), 200

@ns_mini_agro.route('/get_status/')
class GetStatus(Resource):
    @ns_mini_agro.doc('get_status')
    @cross_origin()
    @jwt_required()
    def get(self):
        user_id = get_jwt_identity()
        source_ids = APIUtils.filter_user_source_ids(user_id)
        if not source_ids:
            return jsonify({'error': 'No sources found'}), 400
        nodes = AppDynamicNodeBuilder.load_nodes(source_ids)
        nodes = replace_nans(nodes)
        nodes = DictUtils.to_camel_case(nodes)
        nodes = DictUtils.flatten_dict(nodes)
        js = json.dumps(nodes, default=str)
        return jsonify(json.loads(js)), 200

daily_summary_parser = reqparse.RequestParser()
daily_summary_parser.add_argument('start', type=str, required=True, help='The start date')
daily_summary_parser.add_argument('end', type=str, required=True, help='The end date')
def replace_nans(obj):
    # Check if the object is a dictionary
    if isinstance(obj, dict):
        return {key: replace_nans(val) for key, val in obj.items()}
    # Check if the object is a list
    elif isinstance(obj, list):
        return [replace_nans(item) for item in obj]
    # Check if the object is a float and NaN
    elif isinstance(obj, float) and np.isnan(obj):
        return 0
    else:
        return obj
    
@ns_mini_agro.route('/get_daily_summary/')
class GetDailySummary(Resource):
    @ns_mini_agro.doc('get_daily_summary')
    @cross_origin()
    @jwt_required()
    def get(self):
        user_id = get_jwt_identity()
        args = daily_summary_parser.parse_args()
        start = args.get('start')
        end = args.get('end')
        print(start, end)
        source_ids = APIUtils.filter_user_source_ids(user_id)
        records = AppDailySummaryBuilder.load_multiple_sources( source_ids=source_ids, start=start, end=end,)
        records = replace_nans(records)
        records = DictUtils.to_camel_case(records)
        records = DictUtils.flatten_dict(records)
        js = json.dumps(records, default=str)
        return jsonify(json.loads(js)), 200

range_summary_parser = reqparse.RequestParser()
range_summary_parser.add_argument('start', type=str, required=True, help='The start date')
range_summary_parser.add_argument('end', type=str, required=True, help='The end date')

@ns_mini_agro.route('/get_range_summary/')
class GetRangeSummary(Resource):
    @ns_mini_agro.doc('get_range_summary')
    @cross_origin()
    @jwt_required()
    def get(self):
        user_id = get_jwt_identity()
        
        args = range_summary_parser.parse_args()
        start = args.get('start')
        end = args.get('end')

        if not start or not end:
            return jsonify({'error': 'Invalid date range'}), 400
        end = datetime.strptime(end, '%Y-%m-%d')    
        start = datetime.strptime(start, '%Y-%m-%d')
        print(user_id)
        # yesterday = (datetime.utcnow(), second=0, microsecond=0)
        # end = min(end, datetime.utcnow()-timedelta(days=1), end)
        source_ids = APIUtils.filter_user_source_ids(user_id)   
        pprint.pp(source_ids)
        if not source_ids:
            return jsonify({'error': 'No sources found'}), 400
        records = RangeSummaryRecordBuilder().create_range_summary_records(source_ids=source_ids, start=start, end=end)#, is_last_record=end==datetime.utcnow().date())
        records = replace_nans(records)
        records = DictUtils.to_camel_case(records)
        records = DictUtils.flatten_dict(records)
        js = json.dumps(records, default=str)
        return jsonify(json.loads(js)), 200

@ns_mini_agro.route('/get_report_views/')
class GetReportViews(Resource):
    @ns_mini_agro.doc('get_report_views')
    @cross_origin()
    @jwt_required()
    def get(self):
        user_id = get_jwt_identity()
        source_ids = APIUtils.filter_user_source_ids(user_id)
        if not source_ids:
            return jsonify({'error': 'No sources found'}), 400
        views = ReportView.get_report_views(user_id)
        pprint.pp(views)
        js = json.dumps(views, default=str)
        return jsonify(json.loads(js)), 200

delete_rv_parser = reqparse.RequestParser()
delete_rv_parser.add_argument('report_id', type=str, required=True, help='The report id to delete')

@ns_mini_agro.route('/delete_report_view/')
class DeleteReportView(Resource):
    @ns_mini_agro.doc('delete_report_view')
    @cross_origin()
    @jwt_required()
    def delete(self):
        user_id = get_jwt_identity()
        args = delete_rv_parser.parse_args()
        report_id = args.get('report_id')
        ReportView.delete_report_view(user_id, report_id)
        return jsonify({'message': 'Report view deleted'}), 200


@ns_mini_agro.route('/save_report_view/')
class SaveReportView(Resource):
    @ns_mini_agro.doc('save_report_view')
    @cross_origin()
    @jwt_required()
    def post(self):
        user_id = get_jwt_identity()
        data = request.get_json()
        if data is None:
            return {'error': 'Invalid JSON data or empty request body'}, 400
        report = ReportView(report=data)
        sv = report.save_to_db(user_id)
        if sv:
            return jsonify({'message': 'Report view saved'}), 200
        else:
            return {'error': 'Failed to save report view'}, 400
