class Gamma::Parser::DataParser < Gamma::Parser
  DEFAULT_SYNC_MODE = "replace"

  def initialize(data_yaml_path, filter_root_dir, hook_root_dir, in_client, out_client, apply: false)
    @data_settings = YAML.load_file(data_yaml_path).map(&:with_indifferent_access)
    @filter_root_dir = filter_root_dir
    @hook_root_dir = hook_root_dir
    @in_client = in_client
    @out_client = out_client
    @apply = apply
  end

  def gamma_tables
    exist_tables = database_exist_tables
    @data_settings.map do |d|
      parse_data_settings(d[:data], exist_tables)
    end.flatten
  end

  def parse_data_settings(data, exist_tables)
    tables = if Array(data[:table]).join == "*"
               without = Array(data[:table_without]) || []
               exist_tables.reject { |v| without.include?(v.table_name) }
             else
               Array(data[:table]).map do |table_name|
                 exist_tables.find { |t| t.table_name == table_name }
               end.compact
             end
    tables = tables.map do |t|
      t.sync_mode = data[:mode].presence || DEFAULT_SYNC_MODE
      t.delta_column = data[:delta_column].present? && (t.in_exist_columns & t.out_exist_columns).include?(data[:delta_column]) ? data[:delta_column] : nil
      t.filters = data[:filters].present? ? parse_filters(data[:filters], t) : []
      t.hooks = data[:hooks].present? ? parse_hooks(data[:hooks], t) : []

      t
    end
    tables
  end

  private

  def database_exist_tables
    in_tables = select_table_definitions(@in_client)
    out_tables = select_table_definitions(@out_client)

    (in_tables + out_tables).uniq.map do |table|
      t = Gamma::Table.new
      t.table_name = table
      t.in_exist = in_tables.include?(table)
      t.out_exist = out_tables.include?(table)
      t.in_exist_columns = select_column_definitions(@in_client, table)
      t.out_exist_columns = select_column_definitions(@out_client, table)
      t
    end
  end

  def select_table_definitions(client)
    query = <<-EOS
      SELECT
        *
      FROM
        TABLES
      INNER JOIN
        COLLATION_CHARACTER_SET_APPLICABILITY CCSA
      ON
        TABLES.TABLE_COLLATION = CCSA.COLLATION_NAME
      WHERE
        TABLE_SCHEMA = '#{client.schema_client.escape(client.config[:database])}'
      ORDER BY
        TABLE_NAME
    EOS
    client.schema_client.query(query.strip_heredoc).to_a.map { |v| v["TABLE_NAME"] }
  end

  def select_column_definitions(client, table_name)
    query = <<-EOS
      SELECT
        *
      FROM
        COLUMNS
      WHERE
        TABLE_SCHEMA = '#{client.schema_client.escape(client.config[:database])}'
        AND TABLE_NAME = '#{client.schema_client.escape(table_name)}'
      ORDER BY
        TABLE_NAME, ORDINAL_POSITION
    EOS
    client.schema_client.query(query.strip_heredoc).to_a.map { |v| v["COLUMN_NAME"] }
  end

  def parse_filters(filters, table)
    parse_interceptors(:filter, filters, table)
  end

  def parse_hooks(hooks, table)
    parse_interceptors(:hook, hooks, table)
  end

  def parse_interceptors(interceptor_type, interceptors, table)
    interceptors = interceptors.is_a?(Array) ? interceptors : [interceptors]
    interceptors.map do |interceptor|
      type = if interceptor[:row].present?
               :row
             elsif interceptor[:column].present?
               :column
             end

      if type == :row
        options = interceptor[:row]
        fail "Required scripts arguments. table: #{table.table_name}, #{interceptor_type}_type: #{type}" unless options[:scripts].present?

        Array(options[:scripts]).map do |script|
          h = Gamma.const_get(interceptor_type.to_s.camelize).new
          h.send(:"#{interceptor_type}_type=", :row)
          h.column_name = nil
          h.script_path = script
          h.root_dir = instance_variable_get(:"@#{interceptor_type}_root_dir")
          h.apply = @apply
          h
        end
      elsif type == :column
        options = interceptor[:column]
        fail "Required column name arguments. table: #{table.table_name}, #{interceptor_type}_type: #{type}" unless options[:name].present?
        fail "Required scripts arguments. table: #{table.table_name}, #{interceptor_type}_type: #{type}" unless options[:scripts].present?

        column_names = Array(options[:name])
        scripts = Array(options[:scripts])
        column_names.product(scripts).map do |column_name, script|
          h = Gamma.const_get(interceptor_type.to_s.camelize).new
          h.send(:"#{interceptor_type}_type=", :column)
          h.column_name = column_name
          h.script_path = script
          h.root_dir = instance_variable_get(:"@#{interceptor_type}_root_dir")
          h.apply = @apply
          h
        end
      else
        fail "Unknown #{interceptor_type.to_s.camelize} Type"
      end
    end.flatten.compact
  end
end
