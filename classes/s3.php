<?php
/**
 *
 * Copyright (c) 2008, Donovan Schönknecht.	 All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *	 this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright
 *	 notice, this list of conditions and the following disclaimer in the
 *	 documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * Amazon S3 is a trademark of Amazon.com, Inc. or its affiliates.
 */

/**
 * Amazon S3 PHP class
 *
 * @link http://undesigned.org.za/2007/10/22/amazon-s3-php-class
 * @version 0.4.0
 */
 
namespace S3;
 
class S3 {
	// ACL flags
	const ACL_PRIVATE = 'private';
	const ACL_PUBLIC_READ = 'public-read';
	const ACL_PUBLIC_READ_WRITE = 'public-read-write';
	const ACL_AUTHENTICATED_READ = 'authenticated-read';
	
	public static $use_ss_l = true;
	
	// AWS Access key
	private static $__access_key;
	// AWS Secret key
	private static $__secret_key;
	
	
	/**
	 * Constructor - if you're not using the class statically
	 *
	 * @param string $access_key Access key
	 * @param string $secret_key Secret key
	 * @param boolean $use_ss_l Enable SSL
	 * @return void
	 */
	public function __construct()
	{
		\Config::load('s3', true);
		self::set_auth(\Config::get('s3.access_key_id'), \Config::get('s3.secret_access_key'));
		self::$use_ss_l = \Config::get('s3.enable_ssl');
	}
	
	
	/**
	 * Set AWS access key and secret key
	 *
	 * @param string $access_key Access key
	 * @param string $secret_key Secret key
	 * @return void
	 */
	public static function set_auth($access_key, $secret_key)
	{
		self::$__access_key = $access_key;
		self::$__secret_key = $secret_key;
	}
	
	
	/**
	 * Get a list of buckets
	 *
	 * @param boolean $detailed Returns detailed bucket list when true
	 * @return array | false
	 */
	public static function list_buckets($detailed = false)
	{
		$rest = new S3_Request('GET', '', '');
		$rest = $rest->get_response();
		if ($rest->error === false && $rest->code !== 200)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::list_buckets(): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		$results = array();
		if (!isset($rest->body->buckets))
			return $results;
		
		if ($detailed)
		{
			if (isset($rest->body->owner, $rest->body->owner->ID, $rest->body->owner->display_name))
				$results['owner'] = array('id' => (string)$rest->body->owner->ID, 'name' => (string)$rest->body->owner->ID);
			$results['buckets'] = array();
			foreach ($rest->body->buckets->bucket as $b)
				$results['buckets'][] = array('name' => (string)$b->name, 'time' => strtotime((string)$b->creation_date));
		}
		else
			foreach ($rest->body->buckets->bucket as $b)
				$results[] = (string)$b->name;
		
		return $results;
	}
	
	
	/*
	 * Get contents for a bucket
	 *
	 * If max_Keys is null this method will loop through truncated result sets
	 *
	 * @param string $bucket Bucket name
	 * @param string $prefix Prefix
	 * @param string $marker Marker (last file listed)
	 * @param string $max_keys Max keys (maximum number of keys to return)
	 * @param string $delimiter Delimiter
	 * @param boolean $return_common_prefixes Set to true to return Common_Prefixes
	 * @return array | false
	 */
	public static function get_bucket($bucket, $prefix = null, $marker = null, $max_keys = null, $delimiter = null, $return_common_prefixes = false)
	{
		$rest = new S3_Request('GET', $bucket, '');
		if ($prefix !== null && $prefix !== '')
			$rest->set_parameter('prefix', $prefix);
		if ($marker !== null && $marker !== '')
			$rest->set_parameter('marker', $marker);
		if ($max_keys !== null && $max_keys !== '')
			$rest->set_parameter('max-keys', $max_keys);
		if ($delimiter !== null && $delimiter !== '')
			$rest->set_parameter('delimiter', $delimiter);
		$response = $rest->get_response();
		if ($response->error === false && $response->code !== 200)
			$response->error = array('code' => $response->code, 'message' => 'Unexpected HTTP status');
		if ($response->error !== false)
		{
			trigger_error(sprintf("S3::get_bucket(): [%s] %s", $response->error['code'], $response->error['message']), E_USER_WARNING);
			return false;
		}
		
		$results = array();
		
		$next_marker = null;
		if (isset($response->body, $response->body->contents))
			foreach ($response->body->contents as $c)
			{
				$results[(string)$c->key] = array('name' => (string)$c->key, 'time' => strtotime((string)$c->last_modified), 'size' => (int)$c->size, 'hash' => substr((string)$c->e_tag, 1, -1));
				$next_marker = (string)$c->key;
			}
		
		if ($return_common_prefixes && isset($response->body, $response->body->common_prefixes))
			foreach ($response->body->common_prefixes as $c)
				$results[(string)$c->prefix] = array('prefix' => (string)$c->prefix);
		
		if (isset($response->body, $response->body->is_truncated) && (string)$response->body->is_truncated == 'false')
			return $results;
		
		if (isset($response->body, $response->body->next_marker))
			$next_marker = (string)$response->body->next_marker;
		
		// Loop through truncated results if max_Keys isn't specified
		if ($max_keys == null && $next_marker !== null && (string)$response->body->is_truncated == 'true')
			do
			{
				$rest = new S3_Request('GET', $bucket, '');
				if ($prefix !== null && $prefix !== '')
					$rest->set_parameter('prefix', $prefix);
				$rest->set_parameter('marker', $next_marker);
				if ($delimiter !== null && $delimiter !== '')
					$rest->set_parameter('delimiter', $delimiter);
				
				if (($response = $rest->get_response(true)) == false || $response->code !== 200)
					break;
				
				if (isset($response->body, $response->body->contents))
					foreach ($response->body->contents as $c)
					{
						$results[(string)$c->key] = array('name' => (string)$c->key, 'time' => strtotime((string)$c->last_modified), 'size' => (int)$c->size, 'hash' => substr((string)$c->e_tag, 1, -1));
						$next_marker = (string)$c->key;
					}
				
				if ($return_common_prefixes && isset($response->body, $response->body->common_prefixes))
					foreach ($response->body->common_prefixes as $c)
						$results[(string)$c->prefix] = array('prefix' => (string)$c->prefix);
				
				if (isset($response->body, $response->body->next_marker))
					$next_marker = (string)$response->body->next_marker;
			}
			while ($response !== false && (string)$response->body->is_truncated == 'true');
		
		return $results;
	}
	
	
	/**
	 * Put a bucket
	 *
	 * @param string $bucket Bucket name
	 * @param constant $acl ACL flag
	 * @param string $location Set as "EU" to create buckets hosted in Europe
	 * @return boolean
	 */
	public static function put_bucket($bucket, $acl = self::ACL_PRIVATE, $location = false)
	{
		$rest = new S3_Request('PUT', $bucket, '');
		$rest->set_amz_header('x-amz-acl', $acl);
		
		if ($location !== false)
		{
			$dom = new DOM_Document;
			$create_bucket_configuration = $dom->create_element('CreateBucketConfiguration');
			$location_constraint = $dom->create_element('LocationConstraint', strtoupper($location));
			$create_bucket_configuration->append_child($location_constraint);
			$dom->append_child($create_bucket_configuration);
			$rest->data = $dom->save_xm_l();
			$rest->size = strlen($rest->data);
			$rest->set_header('Content-Type', 'application/xml');
		}
		$rest = $rest->get_response();
		
		if ($rest->error === false && $rest->code !== 200)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::put_bucket({$bucket}, {$acl}, {$location}): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		return true;
	}
	
	
	/**
	 * Delete an empty bucket
	 *
	 * @param string $bucket Bucket name
	 * @return boolean
	 */
	public static function delete_bucket($bucket)
	{
		$rest = new S3_Request('DELETE', $bucket);
		$rest = $rest->get_response();
		if ($rest->error === false && $rest->code !== 204)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::delete_bucket({$bucket}): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		return true;
	}
	
	
	/**
	 * Create input info array for put_Object()
	 *
	 * @param string $file Input file
	 * @param mixed $md5sum Use MD5 hash (supply a string if you want to use your own)
	 * @return array | false
	 */
	public static function input_file($file, $md5sum = true)
	{
		if (!file_exists($file) || !is_file($file) || !is_readable($file))
		{
			trigger_error('S3::input_file(): Unable to open input file: ' . $file, E_USER_WARNING);
			return false;
		}
		return array('file' => $file, 'size' => filesize($file), 'md5sum' => $md5sum !== false ? (is_string($md5sum) ? $md5sum : base64_encode(md5_file($file, true))) : '');
	}
	
	
	/**
	 * Create input array info for put_Object() with a resource
	 *
	 * @param string $resource Input resource to read from
	 * @param integer $buffer_size Input byte size
	 * @param string $md5sum MD5 hash to send (optional)
	 * @return array | false
	 */
	public static function input_resource(&$resource, $buffer_size, $md5sum = '')
	{
		if (!is_resource($resource) || $buffer_size < 0)
		{
			trigger_error('S3::input_resource(): Invalid resource or buffer size', E_USER_WARNING);
			return false;
		}
		$input = array('size' => $buffer_size, 'md5sum' => $md5sum);
		$input['fp'] =& $resource;
		return $input;
	}
	
	
	/**
	 * Put an object
	 *
	 * @param mixed $input Input data
	 * @param string $bucket Bucket name
	 * @param string $uri Object URI
	 * @param constant $acl ACL constant
	 * @param array $meta_headers Array of x-amz-meta-* headers
	 * @param array $request_headers Array of request headers or content type as a string
	 * @return boolean
	 */
	public static function put_object($input, $bucket, $uri, $acl = self::ACL_PRIVATE, $meta_headers = array(), $request_headers = array())
	{
		if ($input === false)
			return false;
		$rest = new S3_Request('PUT', $bucket, $uri);
		
		if (is_string($input))
			$input = array('data' => $input, 'size' => strlen($input), 'md5sum' => base64_encode(md5($input, true)));
		
		// Data
		if (isset($input['fp']))
			$rest->fp =& $input['fp'];
		elseif (isset($input['file']))
			$rest->fp = @fopen($input['file'], 'rb');
		elseif (isset($input['data']))
			$rest->data = $input['data'];
		
		// Content-Length (required)
		if (isset($input['size']) && $input['size'] >= 0)
			$rest->size = $input['size'];
		else
		{
			if (isset($input['file']))
				$rest->size = filesize($input['file']);
			elseif (isset($input['data']))
				$rest->size = strlen($input['data']);
		}
		
		// Custom request headers (Content-Type, Content-Disposition, Content-Encoding)
		if (is_array($request_headers))
			foreach ($request_headers as $h => $v)
				$rest->set_header($h, $v);
		elseif (is_string($request_headers))
			// Support for legacy content_Type parameter
			$input['type'] = $request_headers;
		
		// Content-Type
		if (!isset($input['type']))
		{
			if (isset($request_headers['Content-Type']))
				$input['type'] =& $request_headers['Content-Type'];
			elseif (isset($input['file']))
				$input['type'] = self::__get_mime_type($input['file']);
			else
				$input['type'] = 'application/octet-stream';
		}
		
		// We need to post with Content-Length and Content-Type, MD5 is optional
		if ($rest->size >= 0 && ($rest->fp !== false || $rest->data !== false))
		{
			$rest->set_header('Content-Type', $input['type']);
			if (isset($input['md5sum']))
				$rest->set_header('Content-MD5', $input['md5sum']);
			
			$rest->set_amz_header('x-amz-acl', $acl);
			foreach ($meta_headers as $h => $v)
				$rest->set_amz_header('x-amz-meta-' . $h, $v);
			$rest->get_response();
		}
		else
			$rest->response->error = array('code' => 0, 'message' => 'Missing input parameters');
		
		if ($rest->response->error === false && $rest->response->code !== 200)
			$rest->response->error = array('code' => $rest->response->code, 'message' => 'Unexpected HTTP status');
		if ($rest->response->error !== false)
		{
			trigger_error(sprintf("S3::put_object(): [%s] %s", $rest->response->error['code'], $rest->response->error['message']), E_USER_WARNING);
			return false;
		}
		return true;
	}
	
	
	/**
	 * Put an object from a file (legacy function)
	 *
	 * @param string $file Input file path
	 * @param string $bucket Bucket name
	 * @param string $uri Object URI
	 * @param constant $acl ACL constant
	 * @param array $meta_headers Array of x-amz-meta-* headers
	 * @param string $content_type Content type
	 * @return boolean
	 */
	public static function put_object_file($file, $bucket, $uri, $acl = self::ACL_PRIVATE, $meta_headers = array(), $content_type = null)
	{
		return self::put_object(self::input_file($file), $bucket, $uri, $acl, $meta_headers, $content_type);
	}
	
	
	/**
	 * Put an object from a string (legacy function)
	 *
	 * @param string $string Input data
	 * @param string $bucket Bucket name
	 * @param string $uri Object URI
	 * @param constant $acl ACL constant
	 * @param array $meta_headers Array of x-amz-meta-* headers
	 * @param string $content_type Content type
	 * @return boolean
	 */
	public static function put_object_string($string, $bucket, $uri, $acl = self::ACL_PRIVATE, $meta_headers = array(), $content_type = 'text/plain')
	{
		return self::put_object($string, $bucket, $uri, $acl, $meta_headers, $content_type);
	}
	
	
	/**
	 * Get an object
	 *
	 * @param string $bucket Bucket name
	 * @param string $uri Object URI
	 * @param mixed $save_to Filename or resource to write to
	 * @return mixed
	 */
	public static function get_object($bucket, $uri, $save_to = false)
	{
		$rest = new S3_Request('GET', $bucket, $uri);
		if ($save_to !== false)
		{
			if (is_resource($save_to))
				$rest->fp =& $save_to;
			elseif (($rest->fp = @fopen($save_to, 'wb')) !== false)
				$rest->file = realpath($save_to);
			else
				$rest->response->error = array('code' => 0, 'message' => 'Unable to open save file for writing: ' . $save_to);
		}
		if ($rest->response->error === false)
			$rest->get_response();
		
		if ($rest->response->error === false && $rest->response->code !== 200)
			$rest->response->error = array('code' => $rest->response->code, 'message' => 'Unexpected HTTP status');
		if ($rest->response->error !== false)
		{
			trigger_error(sprintf("S3::get_object({$bucket}, {$uri}): [%s] %s", $rest->response->error['code'], $rest->response->error['message']), E_USER_WARNING);
			return false;
		}
		return $rest->response;
	}
	
	
	/**
	 * Get object information
	 *
	 * @param string $bucket Bucket name
	 * @param string $uri Object URI
	 * @param boolean $return_info Return response information
	 * @return mixed | false
	 */
	public static function get_object_info($bucket, $uri, $return_info = true)
	{
		$rest = new S3_Request('HEAD', $bucket, $uri);
		$rest = $rest->get_response();
		if ($rest->error === false && ($rest->code !== 200 && $rest->code !== 404))
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::get_object_info({$bucket}, {$uri}): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		return $rest->code == 200 ? $return_info ? $rest->headers : true : false;
	}
	
	
	/**
	 * Copy an object
	 *
	 * @param string $bucket Source bucket name
	 * @param string $uri Source object URI
	 * @param string $bucket Destination bucket name
	 * @param string $uri Destination object URI
	 * @param constant $acl ACL constant
	 * @param array $meta_headers Optional array of x-amz-meta-* headers
	 * @param array $request_headers Optional array of request headers (content type, disposition, etc.)
	 * @return mixed | false
	 */
	public static function copy_object($src_bucket, $src_uri, $bucket, $uri, $acl = self::ACL_PRIVATE, $meta_headers = array(), $request_headers = array())
	{
		$rest = new S3_Request('PUT', $bucket, $uri);
		$rest->set_header('Content-Length', 0);
		foreach ($request_headers as $h => $v)
			$rest->set_header($h, $v);
		foreach ($meta_headers as $h => $v)
			$rest->set_amz_header('x-amz-meta-' . $h, $v);
		$rest->set_amz_header('x-amz-acl', $acl);
		$rest->set_amz_header('x-amz-copy-source', sprintf('/%s/%s', $src_bucket, $src_uri));
		if (sizeof($request_headers) > 0 || sizeof($meta_headers) > 0)
			$rest->set_amz_header('x-amz-metadata-directive', 'REPLACE');
		$rest = $rest->get_response();
		if ($rest->error === false && $rest->code !== 200)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::copy_object({$src_bucket}, {$src_uri}, {$bucket}, {$uri}): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		return isset($rest->body->last_modified, $rest->body->e_tag) ? array('time' => strtotime((string)$rest->body->last_modified), 'hash' => substr((string)$rest->body->e_tag, 1, -1)) : false;
	}
	
	
	/**
	 * Set logging for a bucket
	 *
	 * @param string $bucket Bucket name
	 * @param string $target_bucket Target bucket (where logs are stored)
	 * @param string $target_prefix Log prefix (e,g; domain.com-)
	 * @return boolean
	 */
	public static function set_bucket_logging($bucket, $target_bucket, $target_prefix = null)
	{
		// The S3 log delivery group has to be added to the target bucket's ACP
		if ($target_bucket !== null && ($acp = self::get_access_control_policy($target_bucket, '')) !== false)
		{
			// Only add permissions to the target bucket when they do not exist
			$acl_write_set = false;
			$acl_read_set = false;
			foreach ($acp['acl'] as $acl)
				if ($acl['type'] == 'Group' && $acl['uri'] == 'http://acs.amazonaws.com/groups/s3/LogDelivery')
				{
					if ($acl['permission'] == 'WRITE')
						$acl_write_set = true;
					elseif ($acl['permission'] == 'READ_ACP')
						$acl_read_set = true;
				}
			if (!$acl_write_set)
				$acp['acl'][] = array('type' => 'Group', 'uri' => 'http://acs.amazonaws.com/groups/s3/LogDelivery', 'permission' => 'WRITE');
			if (!$acl_read_set)
				$acp['acl'][] = array('type' => 'Group', 'uri' => 'http://acs.amazonaws.com/groups/s3/LogDelivery', 'permission' => 'READ_ACP');
			if (!$acl_read_set || !$acl_write_set)
				self::set_access_control_policy($target_bucket, '', $acp);
		}
		
		$dom = new DOM_Document;
		$bucket_logging_status = $dom->create_element('BucketLoggingStatus');
		$bucket_logging_status->set_attribute('xmlns', 'http://s3.amazonaws.com/doc/2006-03-01/');
		if ($target_bucket !== null)
		{
			if ($target_prefix == null)
				$target_prefix = $bucket . '-';
			$logging_enabled = $dom->create_element('LoggingEnabled');
			$logging_enabled->append_child($dom->create_element('TargetBucket', $target_bucket));
			$logging_enabled->append_child($dom->create_element('TargetPrefix', $target_prefix));
			// TODO: Add Target_Grants?
			$bucket_logging_status->append_child($logging_enabled);
		}
		$dom->append_child($bucket_logging_status);
		
		$rest = new S3_Request('PUT', $bucket, '');
		$rest->set_parameter('logging', null);
		$rest->data = $dom->save_xm_l();
		$rest->size = strlen($rest->data);
		$rest->set_header('Content-Type', 'application/xml');
		$rest = $rest->get_response();
		if ($rest->error === false && $rest->code !== 200)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::set_bucket_logging({$bucket}, {$uri}): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		return true;
	}
	
	
	/**
	 * Get logging status for a bucket
	 *
	 * This will return false if logging is not enabled.
	 * Note: To enable logging, you also need to grant write access to the log group
	 *
	 * @param string $bucket Bucket name
	 * @return array | false
	 */
	public static function get_bucket_logging($bucket)
	{
		$rest = new S3_Request('GET', $bucket, '');
		$rest->set_parameter('logging', null);
		$rest = $rest->get_response();
		if ($rest->error === false && $rest->code !== 200)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::get_bucket_logging({$bucket}): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		// No logging
		if (!isset($rest->body->logging_enabled))
			return false;
		return array('targetBucket' => (string)$rest->body->logging_enabled->target_bucket, 'targetPrefix' => (string)$rest->body->logging_enabled->target_prefix, );
	}
	
	
	/**
	 * Disable bucket logging
	 *
	 * @param string $bucket Bucket name
	 * @return boolean
	 */
	public static function disable_bucket_logging($bucket)
	{
		return self::set_bucket_logging($bucket, null);
	}
	
	
	/**
	 * Get a bucket's location
	 *
	 * @param string $bucket Bucket name
	 * @return string | false
	 */
	public static function get_bucket_location($bucket)
	{
		$rest = new S3_Request('GET', $bucket, '');
		$rest->set_parameter('location', null);
		$rest = $rest->get_response();
		if ($rest->error === false && $rest->code !== 200)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::get_bucket_location({$bucket}): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		return(isset($rest->body[0]) && (string)$rest->body[0] !== '') ? (string)$rest->body[0] : 'US';
	}
	
	
	/**
	 * Set object or bucket Access Control Policy
	 *
	 * @param string $bucket Bucket name
	 * @param string $uri Object URI
	 * @param array $acp Access Control Policy Data (same as the data returned from get_Access_Control_Policy)
	 * @return boolean
	 */
	public static function set_access_control_policy($bucket, $uri = '', $acp = array())
	{
		$dom = new DOM_Document;
		$dom->format_output = true;
		$access_control_policy = $dom->create_element('AccessControlPolicy');
		$access_control_list = $dom->create_element('AccessControlList');
		
		// It seems the owner has to be passed along too
		$owner = $dom->create_element('Owner');
		$owner->append_child($dom->create_element('ID', $acp['owner']['id']));
		$owner->append_child($dom->create_element('DisplayName', $acp['owner']['name']));
		$access_control_policy->append_child($owner);
		
		foreach ($acp['acl'] as $g)
		{
			$grant = $dom->create_element('Grant');
			$grantee = $dom->create_element('Grantee');
			$grantee->set_attribute('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance');
			if (isset($g['id']))
			{
				// Canonical_User (DisplayName is omitted)
				$grantee->set_attribute('xsi:type', 'CanonicalUser');
				$grantee->append_child($dom->create_element('ID', $g['id']));
			}
			elseif (isset($g['email']))
			{
				// Amazon_Customer_By_Email
				$grantee->set_attribute('xsi:type', 'AmazonCustomerByEmail');
				$grantee->append_child($dom->create_element('EmailAddress', $g['email']));
			}
			elseif ($g['type'] == 'Group')
			{
				// Group
				$grantee->set_attribute('xsi:type', 'Group');
				$grantee->append_child($dom->create_element('URI', $g['uri']));
			}
			$grant->append_child($grantee);
			$grant->append_child($dom->create_element('Permission', $g['permission']));
			$access_control_list->append_child($grant);
		}
		
		$access_control_policy->append_child($access_control_list);
		$dom->append_child($access_control_policy);
		
		$rest = new S3_Request('PUT', $bucket, $uri);
		$rest->set_parameter('acl', null);
		$rest->data = $dom->save_xm_l();
		$rest->size = strlen($rest->data);
		$rest->set_header('Content-Type', 'application/xml');
		$rest = $rest->get_response();
		if ($rest->error === false && $rest->code !== 200)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::set_access_control_policy({$bucket}, {$uri}): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		return true;
	}
	
	
	/**
	 * Get object or bucket Access Control Policy
	 *
	 * @param string $bucket Bucket name
	 * @param string $uri Object URI
	 * @return mixed | false
	 */
	public static function get_access_control_policy($bucket, $uri = '')
	{
		$rest = new S3_Request('GET', $bucket, $uri);
		$rest->set_parameter('acl', null);
		$rest = $rest->get_response();
		if ($rest->error === false && $rest->code !== 200)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::get_access_control_policy({$bucket}, {$uri}): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		
		$acp = array();
		if (isset($rest->body->owner, $rest->body->owner->ID, $rest->body->owner->display_name))
		{
			$acp['owner'] = array('id' => (string)$rest->body->owner->ID, 'name' => (string)$rest->body->owner->display_name);
		}
		if (isset($rest->body->access_control_list))
		{
			$acp['acl'] = array();
			foreach ($rest->body->access_control_list->grant as $grant)
			{
				foreach ($grant->grantee as $grantee)
				{
					if (isset($grantee->ID, $grantee->display_name))
						// Canonical_User
						$acp['acl'][] = array('type' => 'CanonicalUser', 'id' => (string)$grantee->ID, 'name' => (string)$grantee->display_name, 'permission' => (string)$grant->permission);
					elseif (isset($grantee->email_address))
						// Amazon_Customer_By_Email
						$acp['acl'][] = array('type' => 'AmazonCustomerByEmail', 'email' => (string)$grantee->email_address, 'permission' => (string)$grant->permission);
					elseif (isset($grantee->URI))
						// Group
						$acp['acl'][] = array('type' => 'Group', 'uri' => (string)$grantee->URI, 'permission' => (string)$grant->permission);
					else
						continue;
				}
			}
		}
		return $acp;
	}
	
	
	/**
	 * Delete an object
	 *
	 * @param string $bucket Bucket name
	 * @param string $uri Object URI
	 * @return boolean
	 */
	public static function delete_object($bucket, $uri)
	{
		$rest = new S3_Request('DELETE', $bucket, $uri);
		$rest = $rest->get_response();
		if ($rest->error === false && $rest->code !== 204)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::delete_object(): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		return true;
	}
	
	
	/**
	 * Get a query string authenticated URL
	 *
	 * @param string $bucket Bucket name
	 * @param string $uri Object URI
	 * @param integer $lifetime Lifetime in seconds
	 * @param boolean $host_bucket Use the bucket name as the hostname
	 * @param boolean $https Use HTTPS ($host_bucket should be false for SSL verification)
	 * @return string
	 */
	public static function get_authenticated_ur_l($bucket, $uri, $lifetime, $host_bucket = false, $https = false)
	{
		$expires = time() + $lifetime;
		// URI should be encoded (thanks Sean O'Dea)
		$uri = str_replace('%2F', '/', rawurlencode($uri));
		return sprintf(($https ? 'https' : 'http') . '://%s/%s?AWSAccessKeyId=%s&Expires=%u&Signature=%s', $host_bucket ? $bucket : $bucket . '.s3.amazonaws.com', $uri, self::$__access_key, $expires, urlencode(self::__get_hash("GET\n\n\n{$expires}\n/{$bucket}/{$uri}")));
	}
	
	/**
	 * Get upload POST parameters for form uploads
	 *
	 * @param string $bucket Bucket name
	 * @param string $uri_prefix Object URI prefix
	 * @param constant $acl ACL constant
	 * @param integer $lifetime Lifetime in seconds
	 * @param integer $max_file_size Maximum filesize in bytes (default 5MB)
	 * @param string $success_redirect Redirect URL or 200 / 201 status code
	 * @param array $amz_headers Array of x-amz-meta-* headers
	 * @param array $headers Array of request headers or content type as a string
	 * @param boolean $flash_vars Includes additional "Filename" variable posted by Flash
	 * @return object
	 */
	public static function get_http_upload_post_params($bucket, $uri_prefix = '', $acl = self::ACL_PRIVATE, $lifetime = 3600, $max_file_size = 5242880, $success_redirect = "201", $amz_headers = array(), $headers = array(), $flash_vars = false)
	{
		// Create policy object
		$policy = new \stdClass();
		$policy->expiration = gmdate('Y-m-d\TH:i:s\Z', (time() + $lifetime));
		$policy->conditions = array();
		$obj = new \stdClass();
		$obj->bucket = $bucket;
		array_push($policy->conditions, $obj);
		$obj = new \stdClass();
		$obj->acl = $acl;
		array_push($policy->conditions, $obj);
		
		// 200 for non-redirect uploads
		$obj = new \stdClass();
		if (is_numeric($success_redirect) && in_array((int)$success_redirect, array(200, 201)))
			$obj->success_action_status = (string)$success_redirect;
		else
			// URL
			$obj->success_action_redirect = $success_redirect;
		array_push($policy->conditions, $obj);
		
		array_push($policy->conditions, array('starts-with', '$key', $uri_prefix));
		if ($flash_vars)
			array_push($policy->conditions, array('starts-with', '$filename', ''));
		foreach (array_keys($headers) as $header_key)
			array_push($policy->conditions, array('starts-with', '$' . $header_key, ''));
		foreach ($amz_headers as $header_key => $header_val)
		{
			$obj = new \stdClass();
			$obj->{$header_key} = (string)$header_val;
			array_push($policy->conditions, $obj);
		}
		array_push($policy->conditions, array('content-length-range', 0, $max_file_size));
		$policy = base64_encode(str_replace('\/', '/', json_encode($policy)));
		
		// Create parameters
		$params = new \stdClass();
		$params->aws_access_key_id = self::$__access_key;
		$params->key = $uri_prefix . '${filename}';
		$params->acl = $acl;
		$params->policy = $policy;
		unset($policy);
		$params->signature = self::__get_hash($params->policy);
		if (is_numeric($success_redirect) && in_array((int)$success_redirect, array(200, 201)))
			$params->success_action_status = (string)$success_redirect;
		else
			$params->success_action_redirect = $success_redirect;
		foreach ($headers as $header_key => $header_val)
			$params->{$header_key} = (string)$header_val;
		foreach ($amz_headers as $header_key => $header_val)
			$params->{$header_key} = (string)$header_val;
		return $params;
	}
	
	/**
	 * Create a Cloud_Front distribution
	 *
	 * @param string $bucket Bucket name
	 * @param boolean $enabled Enabled (true/false)
	 * @param array $cnames Array containing CNAME aliases
	 * @param string $comment Use the bucket name as the hostname
	 * @return array | false
	 */
	public static function create_distribution($bucket, $enabled = true, $cnames = array(), $comment = '')
	{
		// Cloud_Front requires SSL
		self::$use_ss_l = true;
		$rest = new S3_Request('POST', '', '2008-06-30/distribution', 'cloudfront.amazonaws.com');
		$rest->data = self::__get_cloud_front_distribution_config_xm_l($bucket . '.s3.amazonaws.com', $enabled, $comment, (string)microtime(true), $cnames);
		$rest->size = strlen($rest->data);
		$rest->set_header('Content-Type', 'application/xml');
		$rest = self::__get_cloud_front_response($rest);
		
		if ($rest->error === false && $rest->code !== 201)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::create_distribution({$bucket}, " . (int)$enabled . ", '$comment'): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		elseif ($rest->body instanceof Simple_XML_Element)
			return self::__parse_cloud_front_distribution_config($rest->body);
		return false;
	}
	
	
	/**
	 * Get Cloud_Front distribution info
	 *
	 * @param string $distribution_id Distribution ID from list_Distributions()
	 * @return array | false
	 */
	public static function get_distribution($distribution_id)
	{
		// Cloud_Front requires SSL
		self::$use_ss_l = true;
		$rest = new S3_Request('GET', '', '2008-06-30/distribution/' . $distribution_id, 'cloudfront.amazonaws.com');
		$rest = self::__get_cloud_front_response($rest);
		
		if ($rest->error === false && $rest->code !== 200)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::get_distribution($distribution_id): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		elseif ($rest->body instanceof Simple_XML_Element)
		{
			$dist = self::__parse_cloud_front_distribution_config($rest->body);
			$dist['hash'] = $rest->headers['hash'];
			return $dist;
		}
		return false;
	}
	
	
	/**
	 * Update a Cloud_Front distribution
	 *
	 * @param array $dist Distribution array info identical to output of get_Distribution()
	 * @return array | false
	 */
	public static function update_distribution($dist)
	{
		// Cloud_Front requires SSL
		self::$use_ss_l = true;
		$rest = new S3_Request('PUT', '', '2008-06-30/distribution/' . $dist['id'] . '/config', 'cloudfront.amazonaws.com');
		$rest->data = self::__get_cloud_front_distribution_config_xm_l($dist['origin'], $dist['enabled'], $dist['comment'], $dist['callerReference'], $dist['cnames']);
		$rest->size = strlen($rest->data);
		$rest->set_header('If-Match', $dist['hash']);
		$rest = self::__get_cloud_front_response($rest);
		
		if ($rest->error === false && $rest->code !== 200)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::update_distribution({$dist['id']}, " . (int)$enabled . ", '$comment'): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		else
		{
			$dist = self::__parse_cloud_front_distribution_config($rest->body);
			$dist['hash'] = $rest->headers['hash'];
			return $dist;
		}
		return false;
	}
	
	
	/**
	 * Delete a Cloud_Front distribution
	 *
	 * @param array $dist Distribution array info identical to output of get_Distribution()
	 * @return boolean
	 */
	public static function delete_distribution($dist)
	{
		// Cloud_Front requires SSL
		self::$use_ss_l = true;
		$rest = new S3_Request('DELETE', '', '2008-06-30/distribution/' . $dist['id'], 'cloudfront.amazonaws.com');
		$rest->set_header('If-Match', $dist['hash']);
		$rest = self::__get_cloud_front_response($rest);
		
		if ($rest->error === false && $rest->code !== 204)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::delete_distribution({$dist['id']}): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		return true;
	}
	
	
	/**
	 * Get a list of Cloud_Front distributions
	 *
	 * @return array
	 */
	public static function list_distributions()
	{
		// Cloud_Front requires SSL
		self::$use_ss_l = true;
		$rest = new S3_Request('GET', '', '2008-06-30/distribution', 'cloudfront.amazonaws.com');
		$rest = self::__get_cloud_front_response($rest);
		
		if ($rest->error === false && $rest->code !== 200)
			$rest->error = array('code' => $rest->code, 'message' => 'Unexpected HTTP status');
		if ($rest->error !== false)
		{
			trigger_error(sprintf("S3::list_distributions(): [%s] %s", $rest->error['code'], $rest->error['message']), E_USER_WARNING);
			return false;
		}
		elseif ($rest->body instanceof Simple_XML_Element && isset($rest->body->distribution_summary))
		{
			$list = array();
			if (isset($rest->body->marker, $rest->body->max_items, $rest->body->is_truncated))
			{
				//$info['marker'] = (string)$rest->body->marker;
				//$info['maxItems'] = (int)$rest->body->max_items;
				//$info['isTruncated'] = (string)$rest->body->is_truncated == 'true' ? true : false;
			}
			foreach ($rest->body->distribution_summary as $summary)
			{
				$list[(string)$summary->id] = self::__parse_cloud_front_distribution_config($summary);
			}
			return $list;
		}
		return array();
	}
	
	
	/**
	 * Get a Distribution_Config DOM_Document
	 *
	 * @internal Used to create XML in create_Distribution() and update_Distribution()
	 * @param string $bucket Origin bucket
	 * @param boolean $enabled Enabled (true/false)
	 * @param string $comment Comment to append
	 * @param string $caller_reference Caller reference
	 * @param array $cnames Array of CNAME aliases
	 * @return string
	 */
	private static function __get_cloud_front_distribution_config_xm_l($bucket, $enabled, $comment, $caller_reference = '0', $cnames = array())
	{
		$dom = new DOM_Document('1.0', 'UTF-8');
		$dom->format_output = true;
		$distribution_config = $dom->create_element('DistributionConfig');
		$distribution_config->set_attribute('xmlns', 'http://cloudfront.amazonaws.com/doc/2008-06-30/');
		$distribution_config->append_child($dom->create_element('Origin', $bucket));
		$distribution_config->append_child($dom->create_element('CallerReference', $caller_reference));
		foreach ($cnames as $cname)
			$distribution_config->append_child($dom->create_element('CNAME', $cname));
		if ($comment !== '')
			$distribution_config->append_child($dom->create_element('Comment', $comment));
		$distribution_config->append_child($dom->create_element('Enabled', $enabled ? 'true' : 'false'));
		$dom->append_child($distribution_config);
		return $dom->save_xm_l();
	}
	
	
	/**
	 * Parse a Cloud_Front distribution config
	 *
	 * @internal Used to parse the Cloud_Front Distribution_Config node to an array
	 * @param object &$node DOM_Node
	 * @return array
	 */
	private static function __parse_cloud_front_distribution_config(&$node)
	{
		$dist = array();
		if (isset($node->id, $node->status, $node->last_modified_time, $node->domain_name))
		{
			$dist['id'] = (string)$node->id;
			$dist['status'] = (string)$node->status;
			$dist['time'] = strtotime((string)$node->last_modified_time);
			$dist['domain'] = (string)$node->domain_name;
		}
		if (isset($node->caller_reference))
			$dist['callerReference'] = (string)$node->caller_reference;
		if (isset($node->comment))
			$dist['comment'] = (string)$node->comment;
		if (isset($node->enabled, $node->origin))
		{
			$dist['origin'] = (string)$node->origin;
			$dist['enabled'] = (string)$node->enabled == 'true' ? true : false;
		}
		elseif (isset($node->distribution_config))
		{
			$dist = array_merge($dist, self::__parse_cloud_front_distribution_config($node->distribution_config));
		}
		if (isset($node->CNAME))
		{
			$dist['cnames'] = array();
			foreach ($node->CNAME as $cname)
				$dist['cnames'][(string)$cname] = (string)$cname;
		}
		return $dist;
	}
	
	
	/**
	 * Grab Cloud_Front response
	 *
	 * @internal Used to parse the Cloud_Front S3_Request::get_response() output
	 * @param object &$rest S3_Request instance
	 * @return object
	 */
	private static function __get_cloud_front_response(&$rest)
	{
		$rest->get_response();
		if ($rest->response->error === false && isset($rest->response->body) && is_string($rest->response->body) && substr($rest->response->body, 0, 5) == '<?xml')
		{
			$rest->response->body = simplexml_load_string($rest->response->body);
			// Grab Cloud_Front errors
			if (isset($rest->response->body->error, $rest->response->body->error->code, $rest->response->body->error->message))
			{
				$rest->response->error = array('code' => (string)$rest->response->body->error->code, 'message' => (string)$rest->response->body->error->message);
				unset($rest->response->body);
			}
		}
		return $rest->response;
	}
	
	
	/**
	 * Get MIME type for file
	 *
	 * @internal Used to get mime types
	 * @param string &$file File path
	 * @return string
	 */
	public static function __get_mime_type(&$file)
	{
		$type = false;
		// Fileinfo documentation says fileinfo_open() will use the
		// MAGIC env var for the magic file
		if (extension_loaded('fileinfo') && isset($_ENV['MAGIC']) && ($finfo = finfo_open(FILEINFO_MIME, $_ENV['MAGIC'])) !== false)
		{
			if (($type = finfo_file($finfo, $file)) !== false)
			{
				// Remove the charset and grab the last content-type
				$type = explode(' ', str_replace('; charset=', ';charset=', $type));
				$type = array_pop($type);
				$type = explode(';', $type);
				$type = trim(array_shift($type));
			}
			finfo_close($finfo);
			
			// If anyone is still using mime_content_type()
		}
		elseif (function_exists('mime_content_type'))
			$type = trim(mime_content_type($file));
		
		if ($type !== false && strlen($type) > 0)
			return $type;
		
		// Otherwise do it the old fashioned way
		static $exts = array('jpg' => 'image/jpeg', 'gif' => 'image/gif', 'png' => 'image/png', 'tif' => 'image/tiff', 'tiff' => 'image/tiff', 'ico' => 'image/x-icon', 'swf' => 'application/x-shockwave-flash', 'pdf' => 'application/pdf', 'zip' => 'application/zip', 'gz' => 'application/x-gzip', 'tar' => 'application/x-tar', 'bz' => 'application/x-bzip', 'bz2' => 'application/x-bzip2', 'txt' => 'text/plain', 'asc' => 'text/plain', 'htm' => 'text/html', 'html' => 'text/html', 'css' => 'text/css', 'js' => 'text/javascript', 'xml' => 'text/xml', 'xsl' => 'application/xsl+xml', 'ogg' => 'application/ogg', 'mp3' => 'audio/mpeg', 'wav' => 'audio/x-wav', 'avi' => 'video/x-msvideo', 'mpg' => 'video/mpeg', 'mpeg' => 'video/mpeg', 'mov' => 'video/quicktime', 'flv' => 'video/x-flv', 'php' => 'text/x-php');
		$ext = strtolower(pathInfo($file, PATHINFO_EXTENSION));
		return isset($exts[$ext]) ? $exts[$ext] : 'application/octet-stream';
	}
	
	
	/**
	 * Generate the auth string: "AWS Access_Key:Signature"
	 *
	 * @internal Used by S3_Request::get_response()
	 * @param string $string String to sign
	 * @return string
	 */
	public static function __get_signature($string)
	{
		return 'AWS ' . self::$__access_key . ':' . self::__get_hash($string);
	}
	
	
	/**
	 * Creates a HMAC-SHA1 hash
	 *
	 * This uses the hash extension if loaded
	 *
	 * @internal Used by __get_Signature()
	 * @param string $string String to sign
	 * @return string
	 */
	private static function __get_hash($string)
	{
		return base64_encode(extension_loaded('hash') ? hash_hmac('sha1', $string, self::$__secret_key, true) : pack('H*', sha1((str_pad(self::$__secret_key, 64, chr(0x00)) ^ (str_repeat(chr(0x5c), 64))) . pack('H*', sha1((str_pad(self::$__secret_key, 64, chr(0x00)) ^ (str_repeat(chr(0x36), 64))) . $string)))));
	}
}