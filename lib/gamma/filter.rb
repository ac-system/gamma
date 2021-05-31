class Gamma::Filter
  attr_accessor :filter_type, :column_name, :script_path, :root_dir, :apply

  def execute_script(record)
    path = File.join(root_dir, script_path)
    fail "Filter Scripts Not Found. path: #{path}" unless File.exist?(path)

    result = record
    require File.join(root_dir, script_path)

    begin
      klass_name = "#{File.basename(path, ".*").camelize}"
      instance = klass_name.constantize.new
      case self.filter_type.to_s
      when "column"
        record[column_name.to_s] = instance.execute(apply, column_name.to_s, record[column_name.to_s])
      when "row"
        record = instance.execute(apply, record)
      else
        fail "Error"
      end
    rescue => e
      raise "Invalid Filter Class #{klass_name}"
    end

    result
  end
end
