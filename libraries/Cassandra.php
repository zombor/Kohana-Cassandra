<?php
/**
* Cassandra
*
* @package        Cassandra
* @author         Jeremy Bush
* @author         James Bathgate
* @copyright      (c) 2010 Jeremy Bush, James Bathgate
* @license        http://www.opensource.org/licenses/isc-license.txt
*/
class Cassandra_Core {

	protected $config = array();
	protected $transport = NULL;
	protected $client = NULL;
	protected $keyspace = NULL;

	public function __construct($config = FALSE)
	{
		require_once Kohana_Config::get('cassandra.thrift_base').'packages/cassandra/Cassandra.php';
		require_once Kohana_Config::get('cassandra.thrift_base').'packages/cassandra/cassandra_types.php';
		require_once Kohana_Config::get('cassandra.thrift_base').'transport/TSocket.php';
		require_once Kohana_Config::get('cassandra.thrift_base').'protocol/TBinaryProtocol.php';
		require_once Kohana_Config::get('cassandra.thrift_base').'transport/TFramedTransport.php';
		require_once Kohana_Config::get('cassandra.thrift_base').'transport/TBufferedTransport.php';

		if (is_string($config))
		{
			$name = $config;

			// Test the config group name
			if (($config = Kohana::config('cassandra.'.$config)) === NULL)
				throw new Kohana_Exception('The :name: group is not defined in your cassandra configuration.', array(':name:' => $name));
		}
		elseif (is_array($config))
		{
			// Append the default configuration options
			$config += Kohana::config('cassandra.default');
		}
		else
		{
			// Load the default group
			$config = Kohana::config('cassandra.default');
		}

		$this->config = $config;

		$this->keyspace = $this->config['keyspace'];

		$socket = new TSocket($this->config['host'], $this->config['port']);
		$this->transport = new TBufferedTransport($socket, 1024, 1024);
		$protocol = new TBinaryProtocol($this->transport);
		$this->client = new CassandraClient($protocol);
		$this->transport->open();

		Kohana_Log::add('debug', 'Cassandra Library initialized');
	}

	public function insert($columnFamily, $key, $data, $consistency = cassandra_ConsistencyLevel::ZERO)
	{
		$columns = array();

		$time = microtime(TRUE);

		foreach($data as $column_name => $val)
		{
			$column_parent = new cassandra_ColumnOrSuperColumn();
			if( ! is_array($val))
			{
				$column = new cassandra_Column();
				$column->timestamp = $time;
				$column->name = $column_name;
				$column->value = $val != NULL ? $val : '';

				$column_parent->column = $column;
			}
			else
			{
				$super_column = new cassandra_SuperColumn();
				$super_column->name = $column_name;
				$super_column->columns = array();

				foreach($val as $sub_column_name => $sub_val)
				{
					$column = new cassandra_Column();
					$column->timestamp = $time;
					$column->name = $sub_column_name;
					$column->value = $sub_val != NULL ? $sub_val : '';

					$super_column->columns[] = $column;
				}

				$column_parent->super_column = $super_column;
			}

			$columns[] = $column_parent;
		}

		$mutation[$columnFamily] = $columns;
		$this->client->batch_insert($this->keyspace, $key, $mutation, $consistency);
	}

	public function delete($column_family, $key, $column_name, $super_column_name = NULL, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		$columnPath = $this->build_column_path($column_family, $column_name, $super_column_name);
		$time = microtime(true);
		$this->client->remove($this->keyspace, $key, $column_path, $time, $consistency);
	}

	public function fetch_row($column_family, $key, $start = '', $finish = '', $reversed = FALSE, $count = 100, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		$columnParent = $this->build_column_parent($column_family);

		$predicate = $this->build_predicate($start, $finish, $reversed, $count);

		return $this->client->get_slice($this->keyspace, $key, $column_parent, $predicate, $consistency);
	}

	public function fetch_rows($column_family, $keys, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		$column_parent = $this->buildColumnParent($column_family);
		$predicate = $this->build_predicate();
		return $this->client->multiget_slice($this->keyspace, $keys, $column_parent, $predicate, $consistency);
	}

	public function fetch_rows_by_range($column_family, $start_key = '', $end_key = '', $row_count = 100, $reversed = FALSE, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		$column_parent = $this->build_column_parent($column_family);
		$predicate = $this->build_predicate();
		$results = $this->client->get_range_slice($this->keyspace, $column_parent, $predicate, $start_key, $end_key, $row_count, $consistency);
		return $results;
	}

	public function fetch_all($column_family, $row_count = 100, $reversed = FALSE, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		return $this->fetch_rows_by_range($column_family, '', '', $row_count, $reversed, $consistency);
	}

	public function fetch_col($column_family, $key, $column_name, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		$column_path = $this->build_column_path($column_family, $column_name);
		return $this->client->get($this->keyspace, $key, $column_path, $consistency);
	}

	public function build_column_parent($column_family, $super_column_name = NULL)
	{
		if(empty($column_family))
		{
			// TODO: Change this to a kohana exception
			throw new Exception('Column Family must be defined in the ColumnParent');
		}

		$column_parent = new cassandra_ColumnParent();
		$column_parent->column_family = $column_family;
		$column_parent->super_column = $super_column_name;

		return $column_parent;
	}
	
	public function build_predicate($start = '', $finish = '', $reversed = FALSE, $count = 100)
	{
		$slice_range = new cassandra_SliceRange();
		// get all columns
		$slice_range->start = $start;
		$slice_range->finish = $finish;
		$slice_range->reversed = $reversed;
		$slice_range->count = $count;

		$predicate = new cassandra_SlicePredicate();
		$predicate->slice_range = $slice_range;

		return $predicate;
	}

	public function build_column_path($column_family, $column_name, $super_column_name = NULL)
	{
		// TODO: Switch to Kohana exception
		if(empty($column_family))
			throw new Exception('Column Family must be defined in the Column Parent');

		// TODO: Switch to Kohana exception
		if(empty($column_name))
			throw new Exception('Column Name must be defined in the Column Parent');

		$column_path = new cassandra_ColumnPath();
		$column_path->column_family = $column_family;
		$column_path->column = $column_name;
		$column_path->super_column = $super_column_name;

		return $column_path;
	}
}