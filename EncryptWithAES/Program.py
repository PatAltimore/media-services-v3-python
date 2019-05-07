import configparser
import datetime
from os import makedirs, getcwd, path, urandom
import time
import uuid
from urllib import parse

import adal
from msrestazure.azure_active_directory import AdalAuthentication
from msrestazure.azure_cloud import AZURE_PUBLIC_CLOUD

from azure.mgmt.media import *
from azure.mgmt.media.models import *

from azure.storage.blob import BlockBlobService

#pip install pyjwt
import jwt

class Program:
    def __init__(self):
        # Read the settings from config
        self.config = configparser.ConfigParser()
        self.config.read('./settings.ini')
        self.account_name = self.config['DEFAULT']['ACCOUNT_NAME']
        self.resource_group_name = self.config['DEFAULT']['RESOURCE_GROUP_NAME']
        self.transform_name = self.config['DEFAULT']['TRANSFORM_NAME']
        self.content_key_policy_name = self.config['DEFAULT']['CONTENT_KEY_POLICY_NAME']
        self.content_key_identifier_claim_type = self.config['DEFAULT']['CONTENT_KEY_IDENTIFIER_CLAIM_TYPE']
        self.issuer = self.config['DEFAULT']['ISSUER']
        self.audience = self.config['DEFAULT']['AUDIENCE']

        self.token_sign_key = urandom(40)

        client_id = self.config['DEFAULT']['CLIENT']
        key = self.config['DEFAULT']['KEY']
        subscription_id = self.config['DEFAULT']['SUBSCRIPTION_ID']
        tenant_id = self.config['DEFAULT']['TENANT_ID']

        login_endpoint = AZURE_PUBLIC_CLOUD.endpoints.active_directory
        resource = AZURE_PUBLIC_CLOUD.endpoints.active_directory_resource_id
        context = adal.AuthenticationContext(login_endpoint + '/' + tenant_id)
        credentials = AdalAuthentication(
            context.acquire_token_with_client_credentials,
            resource,
            client_id,
            key
        )

        # The AMS Client
        # You can now use this object to perform different operations to your AMS account.
        self.client = AzureMediaServices(credentials, subscription_id)
    
    def get_or_create_content_key_policy(self, resource_group_name, account_name, content_key_policy_name):
        """Create the content key policy that configures how the content key is delivered to end clients 
        via the Key Delivery component of Azure Media Services.

        :param resource_group_name: The name of the resource group within the Azure subscription.
        :param account_name: The Media Services account name.
        :param asset_name: The asset name.
        :param file_to_upload: The file you want to upload into the asset.
        :return Policy
        :rtype: ~azure.mgmt.media.models.Policy
        """
        policy = self.client.content_key_policies.get(resource_group_name, account_name, content_key_policy_name)
        
        if policy is None:
            primary_key = ContentKeyPolicySymmetricTokenKey(key_value = self.token_sign_key)
            alternate_keys = None
            required_claims = [ContentKeyPolicyTokenClaim(claim_type = self.content_key_identifier_claim_type, claim_value = None)]

            option = ContentKeyPolicyOption(configuration = ContentKeyPolicyClearKeyConfiguration(), 
                restriction = ContentKeyPolicyTokenRestriction(issuer = self.issuer, audience = self.audience, primary_verification_key = primary_key,
                        restriction_token_type = ContentKeyPolicyRestrictionTokenType.jwt, alternate_verification_keys = alternate_keys, required_claims = required_claims))

            # Since we are randomly generating the signing key each time, make sure to create or update the policy each time.
            # Normally you would use a long lived key so you would just check for the policies existence with Get instead of
            # ensuring to create or update it each time.
            policy = self.client.content_key_policies.create_or_update(resource_group_name, account_name, content_key_policy_name, [option])

        return policy


    def create_input_asset(self, resource_group_name, account_name, asset_name, file_to_upload):
        """ Creates a new input Asset and uploads the specified local video file into it.

        :param resource_group_name: The name of the resource group within the Azure subscription.
        :param account_name: The Media Services account name.
        :param asset_name: The asset name.
        :param file_to_upload: The file you want to upload into the asset.
        :return Asset
        :rtype: ~azure.mgmt.media.models.Asset
        """
        # In this example, we are assuming that the asset name is unique.
        #
        # If you already have an asset with the desired name, use the Assets.Get method
        # to get the existing asset. In Media Services v3, the Get method on entities returns null 
        # if the entity doesn't exist (a case-insensitive check on the name).

        # Call Media Services API to create an Asset.
        # This method creates a container in storage for the Asset.
        # The files (blobs) associated with the asset will be stored in this container.
        asset = self.client.assets.create_or_update(resource_group_name, account_name, asset_name, Asset())

        # Use Media Services API to get back a response that contains
        # SAS URL for the Asset container into which to upload blobs.
        # That is where you would specify read-write permissions 
        # and the exparation time for the SAS URL.
        expiryTime = datetime.datetime.utcnow() + datetime.timedelta(hours = 4)
        response = self.client.assets.list_container_sas(
            resource_group_name,
            account_name,
            asset_name,
            permissions = AssetContainerPermission.read_write,
            expiry_time= datetime.datetime.utcnow() + datetime.timedelta(hours = 4))

        sasUri = response.asset_container_sas_urls[0]

        # Use Storage API to get a reference to the Asset container
        # that was created by calling Asset's CreateOrUpdate method. 
        parsed_url = parse.urlparse(sasUri)
        storage_account_name = parsed_url.netloc.split('.')[0]

        # Remove the leading /
        container_name = parsed_url.path[1:]
        token = parsed_url.query
        local_path = getcwd()
        block_blob_service = BlockBlobService(account_name = storage_account_name, sas_token = token)
        
        # Use Strorage API to upload the file into the container in storage.
        block_blob_service.create_blob_from_path(container_name, file_to_upload, path.join(local_path, file_to_upload))

        return asset

    def create_output_asset(self, resource_group_name, account_name, asset_name):
        """ Creates an ouput asset. The output from the encoding Job must be written to an Asset.

        :param resource_group_name: The name of the resource group within the Azure subscription.
        :param account_name: The Media Services account name.
        :param asset_name: The asset name.
        :return Asset
        :rtype: ~azure.mgmt.media.models.Asset
        """
        # Check if an Asset already exists
        output_asset = self.client.assets.get(resource_group_name, account_name, asset_name)
        asset = Asset()
        output_asset_name = asset_name

        if output_asset is not None:
            # Name collision! In order to get the sample to work, let's just go ahead and create a unique asset name
            # Note that the returned Asset can have a different name than the one specified as an input parameter.
            # You may want to update this part to throw an Exception instead, and handle name collisions differently.
            uniqueness = uuid.uuid1
            output_asset_name += str(uniqueness)
            
            print('Warning – found an existing Asset with name = ' + asset_name)
            print('Creating an Asset with this name instead: ' + output_asset_name)                

        return self.client.assets.create_or_update(resource_group_name, account_name, output_asset_name, asset)
    
    def get_or_create_transform(self, resource_group_name, account_name, transform_name):
        """If the specified transform exists, get that transform.
        If the it does not exist, creates a new transform with the specified output. 
        In this case, the output is set to encode a video using one of the built-in encoding presets.

        :param resource_group_name: The name of the resource group within the Azure subscription.
        :param account_name: The Media Services account name.
        :param transform_name: The transform name.
        :return Transform
        :rtype: ~azure.mgmt.media.models.Transform
        """        
        # Does a Transform already exist with the desired name? Assume that an existing Transform with the desired name
        # also uses the same recipe or Preset for processing content.
        transform = self.client.transforms.get(resource_group_name, account_name, transform_name)

        if not transform:            

            # The preset for the Transform is set to one of Media Services built-in sample presets.
            # You can  customize the encoding settings by changing this to use "StandardEncoderPreset" class.
            preset = BuiltInStandardEncoderPreset(preset_name = EncoderNamedPreset.adaptive_streaming)

            transformOutput = TransformOutput(preset = preset)

            # You need to specify what you want it to produce as an output
            output = [transformOutput]

            # Create the Transform with the output defined above
            transform = self.client.transforms.create_or_update(resource_group_name, account_name, transform_name, output)

        return transform   

    def submit_job(self, resource_group_name, account_name, transform_name, job_name, inputasset_name, outputasset_name):
        """Submits a request to Media Services to apply the specified Transform to a given input video.

        :param resource_group_name: The name of the resource group within the Azure subscription.
        :param account_name: The Media Services account name.
        :param transform_name: The transform name.
        :param job_name: The job name. 
        :param inputasset_name: The inputAsset name.      
        :param outputasset_name: The outputAsset name.                   
        :return: Job
        :rtype: ~azure.mgmt.media.models.Job
        """ 
        # Use the name of the created input asset to create the job input.
        job_input = JobInputAsset(asset_name = inputasset_name)
        
        job_output = JobOutputAsset(asset_name = outputasset_name)

        job_outputs = [job_output]

        job = Job(input = job_input, outputs = job_outputs)

        # In this example, we are assuming that the job name is unique.
        #
        # If you already have a job with the desired name, use the Jobs.Get method
        # to get the existing job. In Media Services v3, the Get method on entities returns null 
        # if the entity doesn't exist (a case-insensitive check on the name).
        

        job = self.client.jobs.create(
            resource_group_name,
            account_name,
            transform_name,
            job_name,
            job)

        return job

    def wait_for_job_to_finish(self, resource_group_name, account_name, transform_name, job_name):
        """ Polls Media Services for the status of the Job.
        :param resource_group_name: The name of the resource group within the Azure subscription.
        :param account_name: The Media Services account name.
        :param transform_name: The transform name.
        :param job_name: The job name.
        :return: Job
        :rtype: ~azure.mgmt.media.models.Job
        """
        SleepIntervalMs = 5

        while True:
            job = self.client.jobs.get(resource_group_name, account_name, transform_name, job_name)

            print('Job is {}'.format(job.state))

            for i in range(len(job.outputs)):
                output = job.outputs[i]

                print(' JobOutput[{}] is {}'.format(i, output.state))

                if output.state == JobState.processing:
                    print('  Progress: {}'.format(output.progress))

            if job.state == JobState.finished or job.state == JobState.error or job.state == JobState.canceled:
                break

            time.sleep(SleepIntervalMs)

        return job
    def create_streaming_locator(self, resource_group_name, account_name, asset_name, locator_name, content_key_policy_name):
        """ Creates a StreamingLocator for the specified asset and with the specified streaming policy name.
        Once the StreamingLocator is created the output asset is available to clients for playback.
        :param resource_group_name: The name of the resource group within the Azure subscription.
        :param account_name: The Media Services account name.
        :param asset_name: The asset name.
        :param locator_name: The locator name.
        :param content_key_policy_name: The content key policy name
        :return: StreamingLocator
        :rtype: ~azure.mgmt.media.models.StreamingLocator
        """

        locator = StreamingLocator(asset_name = asset_name, 
            streaming_policy_name = 'Predefined_ClearKey', 
            default_content_key_policy_name = content_key_policy_name)

        locator = self.client.streaming_locators.create(
            resource_group_name,
            account_name,
            locator_name,
            locator)

        return locator

    def get_token(self, issuer, audience, key_identifier_claim_type, key_identifier, token_verification_key):
        """ Create a token that will be used to protect your stream.
        Only authorized clients would be able to play the video.  

        :param issuer: The issuer is the secure token service that issues the token.
        :param audience: The audience, sometimes called scope, describes the intent of the token or the resource the token authorizes access to.
        :param key_dentifier_claim_type: The content key identifier claim type.
        :param key_dentifier: The content key ID.
        :param token_erification_key: Contains the key that the token was signed with
        :return: bytes
        """

        payload = {
            'iss': issuer,
            'aud': audience,
            key_identifier_claim_type: key_identifier,
            'nbf': datetime.datetime.utcnow() + datetime.timedelta(minutes = -5),
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours = 1),
        }

        return jwt.encode(payload, token_verification_key, algorithm='HS256').decode('utf-8')



    def get_dash_streaming_url(self, resource_group_name, account_name, locator_name):
        """Checks if the "default" streaming endpoint is in the running state,
        if not, starts it.
        Then, builds the streaming URLs.
        :param resource_group_name: The name of the resource group within the Azure subscription.
        :param account_name: The Media Services account name.
        :param asset_name: The asset name.
        :param locator_name: The locator name.
        :return: str
        """

        DefaultStreamingEndpointName = "default"

        dash_path = ''

        streaming_endpoint = self.client.streaming_endpoints.get(resource_group_name, account_name, DefaultStreamingEndpointName)

        if streaming_endpoint is not None:
            if streaming_endpoint.resource_state != StreamingEndpointResourceState.running:
                self.client.streaming_endpoints.start(resource_group_name, account_name, DefaultStreamingEndpointName)

        paths = self.client.streaming_locators.list_paths(resource_group_name, account_name, locator_name)

        for path in paths.streaming_paths:
            scheme = "https"
            host = streaming_endpoint.host_name

            # Look for just the DASH path and generate a URL for the Azure Media Player to playback the content with the AES token to decrypt.
            # Note that the JWT token is set to expire in 1 hour.
            if path.streaming_protocol == StreamingPolicyStreamingProtocol.dash:
                path = path.paths[0]

                dash_path = parse.urlunparse((scheme, host, path, None, None, None))

        return dash_path

    def clean_up(self, resource_group_name, account_name, transform_name, content_key_policy_name):
        """Deletes the jobs and assets that were created.
        Generally, you should clean up everything except objects 
        that you are planning to reuse (typically, you will reuse Transforms, and you will persist StreamingLocators).
        :param resource_group_name: The name of the resource group within the Azure subscription.
        :param account_name: The Media Services account name.
        :param asset_name: The asset name.
        :param transform_name: The transform name. 
        :param content_key_policy_name: The content key policy name.     
        """

        jobs = self.client.jobs.list(resource_group_name, account_name, transform_name)
        for job in jobs:
            self.client.jobs.delete(resource_group_name, account_name, transform_name, job.name)

        streaming_locators = self.client.streaming_locators.list(resource_group_name, account_name)
        for locator in streaming_locators:
            self.client.streaming_locators.delete(resource_group_name, account_name, locator.name)

        assets = self.client.assets.list(resource_group_name, account_name)
        for asset in assets:
            self.client.assets.delete(resource_group_name, account_name, asset.name)
        
        self.client.content_key_policies.delete(resource_group_name, account_name, content_key_policy_name)

    def run(self):
        """Run the sample"""

        # Your input file name and output foler name for encoding
        input_mp4_file_name = 'ignite.mp4'
        output_folder_name = 'output'

        # Creating a unique suffix so that we don't have name collisions if you run the sample
        # multiple times without cleaning up.
        uniqueness = str(uuid.uuid1())
        job_name = 'job-{}'.format(uniqueness)
        locator_name = 'locator-{}'.format(uniqueness)
        output_asset_name = 'output-{}'.format(uniqueness)
        input_asset_name = 'input-{}'.format(uniqueness)

        # Ensure that you have the desired video analyzer Transform. This is really a one time setup operation.
        transform = self.get_or_create_transform(self.resource_group_name, self.account_name, self.transform_name)

        # Create a new input Asset and upload the specified local video file into it.
        self.create_input_asset(self.resource_group_name, self.account_name, input_asset_name, input_mp4_file_name)

        # Use the name of the created input asset to create the job input.
        job_input = JobInputAsset(asset_name = input_asset_name)

        # Output from the encoding Job must be written to an Asset, so let's create one
        output_asset = self.create_output_asset(self.resource_group_name, self.account_name, output_asset_name)

        job = self.submit_job(self.resource_group_name, self.account_name, self.transform_name, job_name, input_asset_name, output_asset.name)
        # In this demo code, we will poll for Job status
        # Polling is not a recommended best practice for production applications because of the latency it introduces.
        # Overuse of this API may trigger throttling. Developers should instead use Event Grid.
        job = self.wait_for_job_to_finish(self.resource_group_name, self.account_name, self.transform_name, job_name)

        if job.state == JobState.finished:
            print('Job finished.')
            
            # Create the content key policy that configures how the content key is delivered to end clients
            # via the Key Delivery component of Azure Media Services.
            policy = self.get_or_create_content_key_policy(self.resource_group_name, self.account_name, self.content_key_policy_name)

            locator = self.create_streaming_locator(self.resource_group_name, self.account_name, output_asset.name, locator_name, self.content_key_policy_name)
    
            # We are using the ContentKeyIdentifierClaim in the ContentKeyPolicy which means that the token presented
            # to the Key Delivery Component must have the identifier of the content key in it.  Since we didn't specify
            # a content key when creating the StreamingLocator, the system created a random one for us.  In order to 
            # generate our test token we must get the ContentKeyId to put in the ContentKeyIdentifierClaim claim.
            key_identifier = locator.content_keys[0].id

            token = self.get_token(self.issuer, self.audience, self.content_key_identifier_claim_type, key_identifier, self.token_sign_key)

            dash_path = self.get_dash_streaming_url(self.resource_group_name, self.account_name, locator.name)

            print('Copy and paste the following URL in your browser to play back the file in the Azure Media Player.')
            print('Note, the player is set to use the AES token and the Bearer token is specified. ')
            print()
            print('https://ampdemo.azureedge.net/?url={}&aes=true&aestoken=Bearer%3D{}'.format(dash_path, token))
            print()

        print('clean up ...')
        input('press any key to continue')
        self.clean_up(self.resource_group_name, self.account_name, self.transform_name, self.content_key_policy_name)

if __name__ == '__main__':
    Program().run()