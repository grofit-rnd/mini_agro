from datetime import datetime
from time import sleep

from dm_server.downloaders.providers.atomation.atom_api import AtomApi
from dm_server.records.grofit_record.grofit_record import GrofitRecord
from dm_server.records.provider.provider_sens_record import ProviderSensRecord
from dm_server.utils.grofit_id_utils import IDUtils
from dm_server.utils.param_utils import DictUtils


class AtomSyncUtils:
    def __init__(self):
        self.atom_api = AtomApi()

    def get_atom_readings(self, macs, start, end, use_gw_dt=False):
        if not macs:
            return []

        self.atom_api.login()
        records = []
        current_page = 1

        while True:
            print(f'Getting page {current_page}')
            print(start, end, macs)
            response = self.atom_api.get_readings_page(current_page, macs, start, end, use_gw_dt=use_gw_dt)
            if response.status_code == 200:
                data = response.json()['data']
                # print(data)
                if not data['readings_data']:
                    break
                recs = data['readings_data']
                records.extend(recs)
                total_count = data['totalCount']
                print(f'Got {len(records)} records {total_count}')

                # processed_records.extend(self.process_records(recs, upload_time))
                current_page += 1
                if len(recs) < 1000:
                    break
            elif response.status_code == 429:
                print('Too many requests')
                sleep(3 * 60)
            else:
                print(response.status_code)
                break

        print(f'Got {len(records)} records')
        return records

    def process_records(self, records, upload_time=None):
        upload_time = upload_time or datetime.utcnow()
        precords = []
        for r in records:
            # pprint.pp(r)
            source_id = IDUtils.gd_mac_to_source_id(r['mac'])
            stream_id = f'atom-{source_id}'
            gw_read_time = DictUtils.get_datetime(r, 'gw_read_time_utc')
            sample_time = DictUtils.get_datetime(r, 'sample_time_utc')
            ar = ProviderSensRecord(source_id=source_id, stream_id=stream_id, data=r, gw_time=gw_read_time,
                                    sample_time=sample_time, server_time=upload_time, provider_id='atomation')

            precords.append(ar)
        return precords

    def provider_to_sensor_records(self, pr_rec):
        records = []

        # if 'triggers' in provider_record and provider_record['triggers']:
        #     print('triggers', provider_record['triggers'])
        for k, v in pr_rec.data.items():

            sensor_type, sensor_class = AtomApi.get_sensor_type_and_class(k)
            if not sensor_type:
                # print(f'No sensor type for {k}')
                continue
            # sensor_class = sensor_type.sensor_class
            # stream_id = f'si-gr_rec-{sensor_type}-{pr_rec.source_id}'

            rec = GrofitRecord(dt=pr_rec.time_info.sample_time, value=v,
                               source_id=pr_rec.source_id,
                               sensor_class=sensor_class, sensor_type=sensor_type)
            if not rec:
                print('got none rec', pr_rec.to_dict())
            records.append(rec)
        return records
