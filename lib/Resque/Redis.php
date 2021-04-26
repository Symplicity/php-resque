<?php

class ConnectionException extends Exception
{

}

/**
 * Wrap Credis to add namespace support and various helper methods.
 *
 * @package		Resque/Redis
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class Resque_Redis
{
    /**
     * Redis namespace
     * @var string
     */
    private static $defaultNamespace = 'resque:';

    private $server;
    private $database;
    private $driver;

	/**
	 * @var array List of all commands in Redis that supply a key as their
	 *	first argument. Used to prefix keys with the Resque namespace.
	 */
	private $keyCommands = array(
		'exists',
		'del',
		'type',
		'keys',
		'expire',
		'ttl',
		'move',
		'set',
		'setex',
		'get',
		'getset',
		'setnx',
		'incr',
		'incrby',
		'decr',
		'decrby',
		'rpush',
		'lpush',
		'llen',
		'lrange',
		'ltrim',
		'lindex',
		'lset',
		'lrem',
		'lpop',
		'blpop',
		'rpop',
		'sadd',
		'srem',
		'spop',
		'scard',
		'sismember',
		'smembers',
		'srandmember',
		'zadd',
		'zrem',
		'zrange',
		'zrevrange',
		'zrangebyscore',
		'zcard',
		'zscore',
		'zremrangebyscore',
		'sort',
		'watch',
		'ping'
	);
	// sinterstore
	// sunion
	// sunionstore
	// sdiff
	// sdiffstore
	// sinter
	// smove
	// rename
	// rpoplpush
	// mget
	// msetnx
	// mset
	// renamenx

	/**
	 * Set Redis namespace (prefix) default: resque
	 * @param string $namespace
	 */
	public static function prefix($namespace)
	{
	    if (strpos($namespace, ':') === false) {
	        $namespace .= ':';
	    }
	    self::$defaultNamespace = $namespace;
	}

	/**
	 * Resque_Redis constructor.
	 * @param $server
	 * @param null $database
	 * @throws CredisException
	 */
	public function __construct($server, $database = null)
	{
		$this->server = $server;
		$this->database = $database;
		$this->connect();
	}

	/**
	 * @throws CredisException
	 */
	public function connect()
	{
		try {
			if (is_array($this->server)) {
				$this->driver = new Credis_Cluster($this->server);
			} else {
				$port = null;
				$password = null;
				$host = $this->server;

				// If not a UNIX socket path or tcp:// formatted connections string
				// assume host:port combination.
				if (strpos($this->server, '/') === false) {
					$parts = explode(':', $this->server);
					if (isset($parts[1])) {
						$port = $parts[1];
					}
					$host = $parts[0];
				} else {
					if (strpos($this->server, 'redis://') !== false) {
						// Redis format is:
						// redis://[user]:[password]@[host]:[port]
						list($userpwd, $hostport) = explode('@', $this->server);
						$userpwd = substr($userpwd, strpos($userpwd, 'redis://') + 8);
						list($host, $port) = explode(':', $hostport);
						list($user, $password) = explode(':', $userpwd);
					}
				}

				$this->driver = new Credis_Client($host, $port);
				$this->driver->setReadTimeout(5000);
				if (isset($password)) {
					$this->driver->auth($password);
				}
			}

			if ($this->database !== null) {
				$this->driver->select($this->database);
			}
		} catch (\Exception $e) {
			throw new CredisException('Error communicating with Redis: ' . $e->getMessage(), 0, $e);
		}
	}

	/**
	 * Magic method to handle all function requests and prefix key based
	 * operations with the {self::$defaultNamespace} key prefix.
	 *
	 * @param string $name The name of the method called.
	 * @param array $args Array of supplied arguments to the method.
	 * @return mixed Return value from Resident::call() based on the command.
	 */
	public function __call($name, $args) {
		if(in_array($name, $this->keyCommands)) {
            if(is_array($args[0])) {
                foreach($args[0] AS $i => $v) {
                    $args[0][$i] = self::$defaultNamespace . $v;
                }
            } else {
                $args[0] = self::$defaultNamespace . $args[0];
            }
		}

		$connected = true;
		while (true) {
			try {
				if (!$connected) {
					try {
						$this->connect();
						Resque::$redis = null;
					} catch (CredisException $e) {
						throw new ConnectionException($e);
					}
				}

				return $this->driver->__call($name, $args);
			} catch (ConnectionException $e) {
				$connected = false;
				usleep(Resque::DEFAULT_INTERVAL * 1000000);
				continue;
			} catch (Exception $e) {
				try {
					$this->driver->ping();
				} catch (CredisException $e) {
					$connected = false;
					usleep(Resque::DEFAULT_INTERVAL * 1000000);
					continue;
				}

				return false;
			}
		}
	}

    public static function getPrefix()
    {
        return self::$defaultNamespace;
    }

    public static function removePrefix($string)
    {
        $prefix=self::getPrefix();

        if (substr($string, 0, strlen($prefix)) == $prefix) {
            $string = substr($string, strlen($prefix), strlen($string) );
        }
        return $string;
    }
}
