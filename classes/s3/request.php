<?php 

namespace S3;

final class S3_Request
{
	private $verb, $bucket, $uri, $resource = '', $parameters = array(), $amz_headers = array(), $headers = array('Host' => '', 'Date' => '', 'Content-MD5' => '', 'Content-Type' => '');
	public $fp = false, $size = 0, $data = false, $response;
	
	
	/**
	 * Constructor
	 *
	 * @param string $verb Verb
	 * @param string $bucket Bucket name
	 * @param string $uri Object URI
	 * @return mixed
	 */
	function __construct($verb, $bucket = '', $uri = '', $default_host = 's3.amazonaws.com')
	{
		$this->verb = $verb;
		$this->bucket = strtolower($bucket);
		$this->uri = $uri !== '' ? '/' . str_replace('%2F', '/', rawurlencode($uri)) : '/';
		
		if ($this->bucket !== '')
		{
			$this->headers['Host'] = $this->bucket . '.' . $default_host;
			$this->resource = '/' . $this->bucket . $this->uri;
		}
		else
		{
			$this->headers['Host'] = $default_host;
			//$this->resource = strlen($this->uri) > 1 ? '/'.$this->bucket.$this->uri : $this->uri;
			$this->resource = $this->uri;
		}
		$this->headers['Date'] = gmdate('D, d M Y H:i:s T');
		
		$this->response = new \stdClass();
		$this->response->error = false;
	}
	
	
	/**
	 * Set request parameter
	 *
	 * @param string $key Key
	 * @param string $value Value
	 * @return void
	 */
	public function set_parameter($key, $value)
	{
		$this->parameters[$key] = $value;
	}
	
	
	/**
	 * Set request header
	 *
	 * @param string $key Key
	 * @param string $value Value
	 * @return void
	 */
	public function set_header($key, $value)
	{
		$this->headers[$key] = $value;
	}
	
	
	/**
	 * Set x-amz-meta-* header
	 *
	 * @param string $key Key
	 * @param string $value Value
	 * @return void
	 */
	public function set_amz_header($key, $value)
	{
		$this->amz_headers[$key] = $value;
	}
	
	
	/**
	 * Get the S3 response
	 *
	 * @return object | false
	 */
	public function get_response()
	{
		$query = '';
		if (sizeof($this->parameters) > 0)
		{
			$query = substr($this->uri, -1) !== '?' ? '?' : '&';
			foreach ($this->parameters as $var => $value)
				if ($value == null || $value == '')
					$query .= $var . '&';
				// Parameters should be encoded (thanks Sean O'Dea)
				else
					$query .= $var . '=' . rawurlencode($value) . '&';
			$query = substr($query, 0, -1);
			$this->uri .= $query;
			
			if (array_key_exists('acl', $this->parameters) || array_key_exists('location', $this->parameters) || array_key_exists('torrent', $this->parameters) || array_key_exists('logging', $this->parameters))
				$this->resource .= $query;
		}
		$url = ((S3::$use_ss_l && extension_loaded('openssl')) ? 'https://' : 'http://') . $this->headers['Host'] . $this->uri;
		//var_dump($this->bucket, $this->uri, $this->resource, $url);
		
		// Basic setup
		$curl = curl_init();
		curl_setopt($curl, CURLOPT_USERAGENT, 'S3/php');
		
		if (S3::$use_ss_l)
		{
			curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, 2);
			curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, 1);
		}
		
		curl_setopt($curl, CURLOPT_URL, $url);
		
		// Headers
		$headers = array();
		$amz = array();
		foreach ($this->amz_headers as $header => $value)
			if (strlen($value) > 0)
				$headers[] = $header . ': ' . $value;
		foreach ($this->headers as $header => $value)
			if (strlen($value) > 0)
				$headers[] = $header . ': ' . $value;
		
		// Collect AMZ headers for signature
		foreach ($this->amz_headers as $header => $value)
			if (strlen($value) > 0)
				$amz[] = strtolower($header) . ':' . $value;
		
		// AMZ headers must be sorted
		if (sizeof($amz) > 0)
		{
			sort($amz);
			$amz = "\n" . implode("\n", $amz);
		}
		else
			$amz = '';
		
		// Authorization string (CloudFront string_To_Sign should only contain a date)
		$headers[] = 'Authorization: ' . S3::__get_signature($this->headers['Host'] == 'cloudfront.amazonaws.com' ? $this->headers['Date'] : $this->verb . "\n" . $this->headers['Content-MD5'] . "\n" . $this->headers['Content-Type'] . "\n" . $this->headers['Date'] . $amz . "\n" . $this->resource);
		
		curl_setopt($curl, CURLOPT_HTTPHEADER, $headers);
		curl_setopt($curl, CURLOPT_HEADER, false);
		curl_setopt($curl, CURLOPT_RETURNTRANSFER, false);
		curl_setopt($curl, CURLOPT_WRITEFUNCTION, array(&$this, '__response_write_callback'));
		curl_setopt($curl, CURLOPT_HEADERFUNCTION, array(&$this, '__response_header_callback'));
		curl_setopt($curl, CURLOPT_FOLLOWLOCATION, true);
		
		// Request types
		switch ($this->verb)
		{
			case 'GET':
				break;
			case 'PUT':
			case 'POST':
				// POST only used for Cloud_Front
				if ($this->fp !== false)
				{
					curl_setopt($curl, CURLOPT_PUT, true);
					curl_setopt($curl, CURLOPT_INFILE, $this->fp);
					if ($this->size >= 0)
						curl_setopt($curl, CURLOPT_INFILESIZE, $this->size);
				}
				elseif ($this->data !== false)
				{
					curl_setopt($curl, CURLOPT_CUSTOMREQUEST, $this->verb);
					curl_setopt($curl, CURLOPT_POSTFIELDS, $this->data);
					if ($this->size >= 0)
						curl_setopt($curl, CURLOPT_BUFFERSIZE, $this->size);
				}
				else
					curl_setopt($curl, CURLOPT_CUSTOMREQUEST, $this->verb);
				break;
			case 'HEAD':
				curl_setopt($curl, CURLOPT_CUSTOMREQUEST, 'HEAD');
				curl_setopt($curl, CURLOPT_NOBODY, true);
				break;
			case 'DELETE':
				curl_setopt($curl, CURLOPT_CUSTOMREQUEST, 'DELETE');
				break;
			default:
				break;
		}
		
		// Execute, grab errors
		if (curl_exec($curl))
			$this->response->code = curl_getinfo($curl, CURLINFO_HTTP_CODE);
		else
			$this->response->error = array('code' => curl_errno($curl), 'message' => curl_error($curl), 'resource' => $this->resource);
		
		@curl_close($curl);
		
		// Parse body into XML
		if ($this->response->error === false && isset($this->response->headers['type']) && $this->response->headers['type'] == 'application/xml' && isset($this->response->body))
		{
			$this->response->body = simplexml_load_string($this->response->body);
			
			// Grab S3 errors
			if (!in_array($this->response->code, array(200, 204)) && isset($this->response->body->code, $this->response->body->message))
			{
				$this->response->error = array('code' => (string)$this->response->body->code, 'message' => (string)$this->response->body->message);
				if (isset($this->response->body->resource))
					$this->response->error['resource'] = (string)$this->response->body->resource;
				unset($this->response->body);
			}
		}
		
		// Clean up file resources
		if ($this->fp !== false && is_resource($this->fp))
			fclose($this->fp);
		
		return $this->response;
	}
	
	
	/**
	 * CURL write callback
	 *
	 * @param resource &$curl CURL resource
	 * @param string &$data Data
	 * @return integer
	 */
	private function __response_write_callback(&$curl, &$data)
	{
		if ($this->response->code == 200 && $this->fp !== false)
			return fwrite($this->fp, $data);
		else
			$this->response->body .= $data;
		return strlen($data);
	}
	
	
	/**
	 * CURL header callback
	 *
	 * @param resource &$curl CURL resource
	 * @param string &$data Data
	 * @return integer
	 */
	private function __response_header_callback(&$curl, &$data)
	{
		if (($strlen = strlen($data)) <= 2)
			return $strlen;
		if (substr($data, 0, 4) == 'HTTP')
			$this->response->code = (int)substr($data, 9, 3);
		else
		{
			list($header, $value) = explode(': ', trim($data), 2);
			if ($header == 'Last-Modified')
				$this->response->headers['time'] = strtotime($value);
			elseif ($header == 'Content-Length')
				$this->response->headers['size'] = (int)$value;
			elseif ($header == 'Content-Type')
				$this->response->headers['type'] = $value;
			elseif ($header == 'ETag')
				$this->response->headers['hash'] = $value{0} == '"' ? substr($value, 1, -1) : $value;
			elseif (preg_match('/^x-amz-meta-.*$/', $header))
				$this->response->headers[$header] = is_numeric($value) ? (int)$value : $value;
		}
		return $strlen;
	}
}
