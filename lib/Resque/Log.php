<?php
/**
 * Resque default logger PSR-3 compliant
 *
 * @package		Resque/Stat
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class Resque_Log extends Psr\Log\AbstractLogger
{
	public $verbose;
	public $logfile;

	public function __construct($verbose = false, $logfile = false) {
		$this->verbose = $verbose;
		$this->logfile = $logfile;
	}

	/**
	 * Logs with an arbitrary level.
	 *
	 * @param mixed   $level    PSR-3 log level constant, or equivalent string
	 * @param string  $message  Message to log, may contain a { placeholder }
	 * @param array   $context  Variables to replace { placeholder }
	 * @return null
	 */
	public function log($level, $message, array $context = array())
	{
		if ($this->logfile) {
			if (!$loghandle = fopen($this->logfile, 'a')) {
				fwrite(STDOUT, "Cannot open file ($this->logfile)" . PHP_EOL);
				exit;
			}
		}

		if ($this->verbose) {
			$logline = '[' . $level . '] [' . strftime('%T %Y-%m-%d') . '] ' . $this->interpolate($message, $context) . PHP_EOL;
			if ($this->logfile) {
				if (fwrite($loghandle, $logline) === FALSE) {
					fwrite(STDOUT, "Cannot write to file ($filename)" . PHP_EOL);
					exit;
				}
				fclose($loghandle);
			} else {
				fwrite(STDOUT, $logline);
			}
			return;
		}

		if (!($level === Psr\Log\LogLevel::INFO || $level === Psr\Log\LogLevel::DEBUG)) {
			$logline = '[' . $level . '] [' . strftime('%Y-%m-%d %T') . '] ' . $this->interpolate($message, $context) . PHP_EOL;
			if ($this->logfile) {
				if (fwrite($loghandle, $logline) === FALSE) {
					fwrite(STDOUT, "Cannot write to file ($filename)" . PHP_EOL);
					exit;
				}
				fclose($loghandle);
			} else {
				fwrite(STDOUT, $logline);
			}
		}
	}

	/**
	 * Fill placeholders with the provided context
	 * @author Jordi Boggiano j.boggiano@seld.be
	 *
	 * @param  string  $message  Message to be logged
	 * @param  array   $context  Array of variables to use in message
	 * @return string
	 */
	public function interpolate($message, array $context = array())
	{
		// build a replacement array with braces around the context keys
		$replace = array();
		foreach ($context as $key => $val) {
			$replace['{' . $key . '}'] = $val;
		}

		// interpolate replacement values into the message and return
		return strtr($message, $replace);
	}
}
