import configparser
import datetime
from os import makedirs, getcwd, path, urandom
import time
import uuid
from urllib import parse
from base64 import b64decode

import adal
from msrestazure.azure_active_directory import AdalAuthentication
from msrestazure.azure_cloud import AZURE_PUBLIC_CLOUD

from azure.mgmt.media import *
from azure.mgmt.media.models import *

from azure.storage.blob import BlockBlobService

class Program:
    def __init__(self):
        # Read the settings from config
        self.config = configparser.ConfigParser()
        self.config.read('./settings.ini')
        self.account_name = self.config['DEFAULT']['ACCOUNT_NAME']
        self.resource_group_name = self.config['DEFAULT']['RESOURCE_GROUP_NAME']

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


    def create_live_event(self, resource_group_name, account_name, live_event_name):
        """Submits a request to Media Services to create live event.

        :param resource_group_name: The name of the resource group within the Azure subscription.
        :type resource_group_name: str
        :param account_name: The Media Services account name.
        :type account_name: str
        :param live_event_name: The live event name. 
        :type live_event_name: str                 
        :return: LiveEvent
        :rtype: ~azure.mgmt.media.models.LiveEvent
        """ 

        media_service = self.client.mediaservices.get(resource_group_name, account_name)

        print('Creating a live event named {}'.format(live_event_name))

        # Note: When creating a LiveEvent, you can specify allowed IP addresses in one of the following formats:                 
        #      IpV4 address with 4 numbers
        #      CIDR address range

        all_allow_ip_range = IPRange(name = 'AllowAll', address = '0.0.0.0', subnet_prefix_length = 0)

        access_control = IPAccessControl(allow = [all_allow_ip_range])

        # Create the LiveEvent input IP access control.
        live_event_input_access = LiveEventInputAccessControl(ip = access_control)

        # Create the LiveEvent Preview IP access control
        preview_ip_access_control = LiveEventPreviewAccessControl( ip = access_control)

        live_event_preview = LiveEventPreview(access_control = preview_ip_access_control)

        # To get the same ingest URL for the same LiveEvent name:
        # 1. Set vanityUrl to true so you have ingest like: 
        #        rtmps://liveevent-hevc12-eventgridmediaservice-usw22.channel.media.azure.net:2935/live/522f9b27dd2d4b26aeb9ef8ab96c5c77           
        # 2. Set accessToken to a desired GUID string (with or without hyphen)

        encoding = LiveEventEncoding(
            # Set this to Standard to enable a transcoding LiveEvent, and None to enable a pass-through LiveEvent
            encoding_type = LiveEventEncodingType.none,
            preset_name = None
        )

        live_event_input = LiveEventInput(streaming_protocol = LiveEventInputProtocol.rtmp, access_control = live_event_input_access)

        # Set this to Default or Low Latency
        # When using Low Latency mode, you must configure the Azure Media Player to use the 
        # quick start hueristic profile or you won't notice the change. 
        # In the AMP player client side JS options, set -  heuristicProfile: "Low Latency Heuristic Profile". 
        # To use low latency optimally, you should tune your encoder settings down to 1 second GOP size instead of 2 seconds.
        stream_options = [StreamOptionsFlag.low_latency]

        live_event = LiveEvent(
                location = media_service.location,
                description = 'Sample LiveEvent for testing',
                vanity_url = False,
                encoding = encoding,
                input = live_event_input,
                preview = live_event_preview,
                stream_options = stream_options
            )

        print('Creating the LiveEvent, be patient this can take time...')

        # When autostart is set to true, the Live Event will be started after creation. 
        # That means, the billing starts as soon as the Live Event starts running. 
        # You must explicitly call Stop on the Live Event resource to halt further billing.
        # The following operation can sometimes take awhile. Be patient.
        return self.client.live_events.create(resource_group_name, self.account_name, live_event_name, live_event, auto_start = True)

    def clean_up_live_event_and_live_output(self, resource_group_name, account_name, live_event_name):
        """Deletes the liveevent and liveoutput that were created.
        Generally, you should clean up everything except objects 
        that you are planning to reuse (typically, you will reuse Transforms, and you will persist StreamingLocators).
        :param resource_group_name: The name of the resource group within the Azure subscription.
        :type resource_group_name: str
        :param account_name: The Media Services account name.
        :type account_name: str
        :param live_event_name: The live event name.
        :type live_event_name:str   
        """

        live_event = self.client.live_events.get(self.resource_group_name, self.account_name, live_event_name)

        if live_event != None:
            if live_event.resource_state == LiveEventResourceState.running:
                # If the LiveEvent is running, stop it and have it remove any LiveOutputs
                self.client.live_events.stop(self.resource_group_name, self.account_name, live_event_name, remove_outputs_on_stop = True)

                # Wait for liveevent state change
                time.sleep(10)
            
            # Delete the LiveEvent
            self.client.live_events.delete(self.resource_group_name, self.account_name, live_event_name)

    def clean_up_locator_and_asset(self, resource_group_name, account_name, locator_name, asset_name):
        """Deletes the locator and asset that were created.
        Generally, you should clean up everything except objects 
        that you are planning to reuse (typically, you will reuse Transforms, and you will persist StreamingLocators).
        :param resource_group_name: The name of the resource group within the Azure subscription.
        :type resource_group_name: str
        :param account_name: The Media Services account name.
        :type account_name: str
        :param locator_name: The locator name.
        :type locator_name: str
        :param asset_name: The asset name.
        type asset_name: str   
        """      
        # Delete the Streaming Locator
        self.client.streaming_locators.delete(self.resource_group_name, self.account_name, locator_name)

        # Delete the Archive Asset
        self.client.assets.delete(self.resource_group_name, self.account_name, asset_name)

    def clean_up_streaming_endpoint(self, resource_group_name, account_name, streaming_endpoint_name, asset_name, stop_endpoint, delete_endpoint):
        """Deletes the streaming endpoint that were created.
        Generally, you should clean up everything except objects 
        that you are planning to reuse (typically, you will reuse Transforms, and you will persist StreamingLocators).
        :param resource_group_name: The name of the resource group within the Azure subscription.
        :type resource_group_name: str
        :param account_name: The Media Services account name.
        :type account_name: str
        :param streaming_endpoint_name: The streaming endpoint name.
        :type streaming_endpoint_name: str
        :param stop_endpoint: If to stop the streaming endpoint.
        :type stop_endpoint: boolean
        :param delete_endpoint: If to delete the streaming endpoint
        :type delete_endpoint: boolean
        """ 

        if stop_endpoint or delete_endpoint:
            streaming_endpoint = self.client.streaming_endpoints.get(self.resource_group_name, self.account_name, streaming_endpoint_name)

            if streaming_endpoint != None and streaming_endpoint.resource_state == StreamingEndpointResourceState.running:
                # Stop the StreamingEndpoint
                self.client.streaming_endpoints.stop(self.resource_group_name, self.account_name, streaming_endpoint_name)

            if delete_endpoint:
                # Delete the StreamingEndpoint
                self.client.streaming_endpoints.delete(self.resource_group_name, self.account_name, streaming_endpoint_name)

    def run(self):
        """Run the sample"""

        # Creating a unique suffix so that we don't have name collisions if you run the sample
        # multiple times without cleaning up.
        try:
            uniqueness = str(uuid.uuid1())[:13]
            live_event_name = 'liveevent-{}'.format(uniqueness)
            asset_name = 'archiveasset-{}'.format(uniqueness)
            live_output_name = 'liveoutput-{}'.format(uniqueness)
            streaming_locator_name = 'streaminglocator-{}'.format(uniqueness)
            streaming_endpoint_name = 'default'

            live_event = self.create_live_event(self.resource_group_name, self.account_name, live_event_name)

            while True:
                live_event = self.client.live_events.get(self.resource_group_name, self.account_name, live_event_name)
                if live_event != None:
                    break
                time.sleep(5)
                

            # Get the input endpoint to configure the on premise encoder with
            ingest_url = live_event.input.endpoints[0].url
            print('The ingest url to configure the on premise encoderwith is: {}'.format(ingest_url))

            # Use the previewEndpoint to preview and verify
            # that the input from the encoder is actually being received
            preview_endpoint = live_event.preview.endpoints[0].url
            print('The preview url is {}'.format(preview_endpoint))

            print('Open the live preview in your browser and use the Azure Media Player to monitor the preview playback:')
            print('\thttps://ampdemo.azureedge.net/?url={}&heuristicprofile=lowlatency'.format(preview_endpoint))

            # Create an Asset for the LiveOutput to use
            print('Creating an asset named {}'.format(asset_name))

            asset = self.client.assets.create_or_update(self.resource_group_name, self.account_name, asset_name, Asset())

            # Crate the liveoutput
            manifest_name = 'output'
            print('Creating a live output named {}'.format(live_output_name))

            liveOutput = LiveOutput(asset_name = asset.name, manifest_name = manifest_name, archive_window_length = datetime.timedelta(minutes = 10))
            liveOutput = self.client.live_outputs.create(self.resource_group_name, self.account_name, live_event_name, live_output_name, liveOutput)

            # Create the StreamingLocator
            print('Creating a streaming locator named {}'.format(streaming_locator_name))

            locator = StreamingLocator(asset_name = asset.name, streaming_policy_name = 'Predefined_ClearStreamingOnly')
            locator = self.client.streaming_locators.create(self.resource_group_name, self.account_name, streaming_locator_name, locator)

            # Get the default Streaming Endpoint on the account
            streaming_endpoint = self.client.streaming_endpoints.get(self.resource_group_name, self.account_name, streaming_endpoint_name)

            #If it's not running, Start it. 
            if streaming_endpoint.resource_state != StreamingEndpointResourceState.running:
                print('Streaming Endpoint was Stopped, restarting now..')
                self.client.streaming_endpoints.start(self.resource_group_name, self.account_name, streaming_endpoint_name)

            # Get the url to stream the output
            paths = self.client.streaming_locators.list_paths(self.resource_group_name, self.account_name, streaming_locator_name)

            print('The urls to stream the output from a client:')

            player_path = ''
            string_builder = ''
            for streaming_path in paths.streaming_paths:
                scheme = "https"
                host = streaming_endpoint.host_name
                
                if len(streaming_path.paths) > 0:
                    string_builder += '\t{}-{}\t\t'.format(streaming_path.streaming_protocol, streaming_path.encryption_scheme)
                    path = streaming_path.paths[0]
                    string_builder += parse.urlunparse((scheme, host, path, None, None, None))
                    
                    if streaming_path.streaming_protocol == StreamingPolicyStreamingProtocol.dash:
                        player_path = parse.urlunparse((scheme, host, path, None, None, None))
            
            if len(string_builder) > 0:
                print('Open the following URL to playback the published,recording LiveOutput in the Azure Media Player')
                print('\t https://ampdemo.azureedge.net/?url={}&heuristicprofile=lowlatency'.format(player_path))

                print('Continue experimenting with the stream until you are ready to finish.')
                input('Press enter to stop the LiveOutput...')

                self.clean_up_live_event_and_live_output(self.resource_group_name, self.account_name, live_event_name)

                print('The LiveOutput and LiveEvent are now deleted.  The event is available as an archive and can still be streamed.')
                input('Press enter to finish cleanup...')
            else:
                print('No Streaming Paths were detected.  Has the Stream been started?');
                print("Cleaning up and Exiting...")
        
        except ApiErrorException as ex:
            print('Hit ApiErrorException')
            print(ex)
        
        finally:
            self.clean_up_live_event_and_live_output(self.resource_group_name, self.account_name, live_event_name)
            self.clean_up_locator_and_asset(self.resource_group_name, self.account_name, streaming_locator_name, asset_name)

if __name__ == '__main__':
    Program().run()