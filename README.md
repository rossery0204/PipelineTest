## Mô tả tổng quan data pipeline: 
1. Data được lấy về qua link định dạng ndjson và được lưu vào thư mục local
2. Data sau đó được chunk nhỏ chuyển sang định dạng csv và gzip
3. Sử dụng Airflow để đẩy các files gzip lên GCS
4. Tạo bảng nếu chưa tồn tại và đẩy dữ liệu từ GCS vào BigQuery
5. Các file trên local sau đó được xóa đi bớt để tiết kiệm bộ nhớ
6. Quá trình được lập lịch chạy vào 7h hàng ngày

## Chi tiết các file
# data_download.py
Dữ liệu được lấy từ link bằng multithreads ThreadPoolExecutor. sau đó được lưu vào cùng một file.
# transform_file.py
Chuyển đổi file từ ndjson sang json rồi chunk nhỏ và convert sang csv và lưu trữ vào thư mục chứ các files csv. Các files csv này sau đó được gzip lại và lưu trữ ở thư mục khác.
# delete_after_done
Xóa thư mục và files csv và gzip
# pipeline.py
Định nghĩa DAG với các tasks là 3 file trên và thêm 3 tasks:
  1. upload_to_gcs: dùng để upload gzip files lên GCS 
  2. create_bq_table: tạo table trên BigQuery nếu chưa có
  3. gcs_to_bq_load: dùng để upload files từ GCS lên BigQuery
đặt lịch chạy 7h hàng ngày.
Dependencies:
data_download >> transform_file >> upload_to_gcs >> delete_after_done
data_download >> transform_file >> upload_to_gcs >> create_bq_table >> gcs_to_bq_load

## Một số Hạn chế
1. Quá trình chuyển files từ ndjson --> csv --> gz đều được lưu trữ có thể gây lãng phí bộ nhớ.
2. Chưa xử lý được file ndjson mà mỗi dòng có cấu trúc khác nhau
3. Chưa chạy được thực tế
