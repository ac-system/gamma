class Gamma::Table
  attr_accessor :table_name, :in_exist, :out_exist, :in_exist_columns, :out_exist_columns
  attr_accessor :sync_mode, :delta_column
  attr_accessor :filters, :hooks

  def record_value(record, type: :out)
    interceptor_type = type == :out ? :hook : :filter
    row_interceptors = send(:"#{interceptor_type}s").select { |i| i.send(:"#{interceptor_type}_type").to_s == "row" }
    column_interceptors = send(:"#{interceptor_type}s").select { |i| i.send(:"#{interceptor_type}_type").to_s == "column" }

    result = record
    result = row_interceptors.reduce(record) { |rec, i| i.execute_script(rec) } if row_interceptors.present?
    result = column_interceptors.reduce(record) { |rec, i| i.execute_script(rec) } if column_interceptors.present?

    result
  end
end
