"""
Home Assistant sACN (E1.31) Integration
Custom component for sending and receiving sACN data

Place this in: custom_components/sacn/
"""

import asyncio
import socket
import struct
import uuid
import logging
from typing import Dict, List, Optional, Callable
from datetime import datetime

import voluptuous as vol
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.entity import Entity
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.const import CONF_HOST, CONF_PORT, CONF_NAME

_LOGGER = logging.getLogger(__name__)

DOMAIN = "sacn"
DEFAULT_PORT = 5568
MULTICAST_BASE = "239.255.0.0"

# Configuration schema
PLATFORM_SCHEMA = vol.Schema({
    vol.Required(CONF_NAME): cv.string,
    vol.Optional(CONF_HOST, default="0.0.0.0"): cv.string,
    vol.Optional(CONF_PORT, default=DEFAULT_PORT): cv.port,
    vol.Optional("universe", default=1): vol.Range(min=1, max=63999),
    vol.Optional("priority", default=100): vol.Range(min=0, max=200),
    vol.Optional("source_name", default="Home Assistant"): cv.string,
})

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Optional("sender"): vol.Schema({
            vol.Optional("source_name", default="Home Assistant"): cv.string,
            vol.Optional("priority", default=100): vol.Range(min=0, max=200),
        }),
        vol.Optional("receivers"): vol.All(cv.ensure_list, [PLATFORM_SCHEMA]),
    })
}, extra=vol.ALLOW_EXTRA)


class sACNPacket:
    """sACN (E1.31) packet structure"""
    
    def __init__(self, universe: int, data: bytes, priority: int = 100, 
                 source_name: str = "Home Assistant", sequence: int = 0):
        self.universe = universe
        self.data = data
        self.priority = priority
        self.source_name = source_name
        self.sequence = sequence
        self.source_cid = uuid.uuid4().bytes
    
    def pack(self) -> bytes:
        """Pack the sACN packet into bytes"""
        # Root Layer
        root_preamble_size = struct.pack("!H", 0x0010)
        root_postamble_size = struct.pack("!H", 0x0000)
        root_acn_packet_identifier = b"\x41\x53\x43\x2d\x45\x31\x2e\x31\x37\x00\x00\x00"
        
        # Frame Layer
        frame_vector = struct.pack("!L", 0x00000002)
        frame_source_name = self.source_name.encode('utf-8')[:64].ljust(64, b'\x00')
        frame_priority = struct.pack("!B", self.priority)
        frame_sync_addr = struct.pack("!H", 0x0000)
        frame_sequence_number = struct.pack("!B", self.sequence)
        frame_options = struct.pack("!B", 0x00)
        frame_universe = struct.pack("!H", self.universe)
        
        # DMP Layer
        dmp_vector = struct.pack("!B", 0x02)
        dmp_address_type = struct.pack("!B", 0xa1)
        dmp_first_property_address = struct.pack("!H", 0x0000)
        dmp_address_increment = struct.pack("!H", 0x0001)
        dmp_property_value_count = struct.pack("!H", len(self.data) + 1)
        dmp_property_values = b'\x00' + self.data  # Start code + DMX data
        
        # Calculate lengths
        dmp_length = 10 + len(dmp_property_values)
        frame_length = 77 + dmp_length
        root_length = 22 + frame_length
        
        # Pack everything
        packet = (
            root_preamble_size + root_postamble_size + root_acn_packet_identifier +
            struct.pack("!H", root_length | 0x7000) + frame_vector + self.source_cid +
            struct.pack("!H", frame_length | 0x7000) + frame_source_name +
            frame_priority + frame_sync_addr + frame_sequence_number + frame_options +
            frame_universe + struct.pack("!H", dmp_length | 0x7000) + dmp_vector +
            dmp_address_type + dmp_first_property_address + dmp_address_increment +
            dmp_property_value_count + dmp_property_values
        )
        
        return packet
    
    @classmethod
    def unpack(cls, data: bytes) -> Optional['sACNPacket']:
        """Unpack bytes into sACN packet"""
        try:
            if len(data) < 126:
                return None
            
            # Verify ACN packet identifier
            if data[4:16] != b"\x41\x53\x43\x2d\x45\x31\x2e\x31\x37\x00\x00\x00":
                return None
            
            # Extract universe
            universe = struct.unpack("!H", data[113:115])[0]
            
            # Extract source name
            source_name = data[44:108].rstrip(b'\x00').decode('utf-8', errors='ignore')
            
            # Extract priority
            priority = data[108]
            
            # Extract sequence
            sequence = data[111]
            
            # Extract DMX data (skip start code)
            dmx_data = data[126:]
            
            return cls(universe, dmx_data, priority, source_name, sequence)
            
        except Exception as e:
            _LOGGER.error(f"Failed to unpack sACN packet: {e}")
            return None


class sACNSender:
    """sACN sender for transmitting DMX data"""
    
    def __init__(self, source_name: str = "Home Assistant", priority: int = 100):
        self.source_name = source_name
        self.priority = priority
        self.sequence = 0
        self.socket = None
        self.active_universes = set()
    
    async def start(self):
        """Start the sender"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
    async def stop(self):
        """Stop the sender"""
        if self.socket:
            self.socket.close()
            self.socket = None
    
    async def send_dmx(self, universe: int, data: bytes):
        """Send DMX data to a universe"""
        if not self.socket:
            await self.start()
        
        # Pad data to 512 channels if needed
        dmx_data = data[:512].ljust(512, b'\x00')
        
        packet = sACNPacket(universe, dmx_data, self.priority, self.source_name, self.sequence)
        packet_data = packet.pack()
        
        # Send to multicast address
        multicast_ip = f"239.255.{(universe >> 8) & 0xFF}.{universe & 0xFF}"
        
        try:
            self.socket.sendto(packet_data, (multicast_ip, DEFAULT_PORT))
            self.active_universes.add(universe)
            self.sequence = (self.sequence + 1) % 256
        except Exception as e:
            _LOGGER.error(f"Failed to send sACN data: {e}")


class sACNReceiver:
    """sACN receiver for receiving DMX data"""
    
    def __init__(self, universes: List[int], callback: Callable = None):
        self.universes = universes
        self.callback = callback
        self.socket = None
        self.running = False
        self.task = None
        self.last_data = {}
    
    async def start(self):
        """Start the receiver"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("", DEFAULT_PORT))
        
        # Join multicast groups for each universe
        for universe in self.universes:
            multicast_ip = f"239.255.{(universe >> 8) & 0xFF}.{universe & 0xFF}"
            mreq = struct.pack("4s4s", socket.inet_aton(multicast_ip), socket.inet_aton("0.0.0.0"))
            self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        self.running = True
        self.task = asyncio.create_task(self._receive_loop())
    
    async def stop(self):
        """Stop the receiver"""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        if self.socket:
            self.socket.close()
            self.socket = None
    
    async def _receive_loop(self):
        """Main receive loop"""
        self.socket.settimeout(0.1)
        
        while self.running:
            try:
                data, addr = self.socket.recvfrom(1144)  # Max sACN packet size
                packet = sACNPacket.unpack(data)
                
                if packet and packet.universe in self.universes:
                    self.last_data[packet.universe] = {
                        'data': packet.data,
                        'source': packet.source_name,
                        'priority': packet.priority,
                        'timestamp': datetime.now()
                    }
                    
                    if self.callback:
                        await self.callback(packet.universe, packet.data, packet.source_name)
                        
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    _LOGGER.error(f"Error in sACN receive loop: {e}")
                break


class sACNEntity(Entity):
    """Base sACN entity"""
    
    def __init__(self, name: str, universe: int):
        self._name = name
        self._universe = universe
        self._state = None
        self._attributes = {}
    
    @property
    def name(self):
        return self._name
    
    @property
    def state(self):
        return self._state
    
    @property
    def extra_state_attributes(self):
        return self._attributes
    
    @property
    def unique_id(self):
        return f"sacn_{self._universe}_{self._name}"


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the sACN integration"""
    
    hass.data[DOMAIN] = {
        'sender': None,
        'receivers': {},
        'entities': {}
    }
    
    # Initialize sender if configured
    sender_config = config.get(DOMAIN, {}).get('sender', {})
    if sender_config:
        sender = sACNSender(
            source_name=sender_config.get('source_name', 'Home Assistant'),
            priority=sender_config.get('priority', 100)
        )
        hass.data[DOMAIN]['sender'] = sender
        await sender.start()
    
    # Register services
    async def send_dmx_service(call: ServiceCall):
        """Service to send DMX data"""
        sender = hass.data[DOMAIN]['sender']
        if not sender:
            _LOGGER.error("sACN sender not configured")
            return
        
        universe = call.data.get('universe', 1)
        channels = call.data.get('channels', {})
        
        # Convert channel dict to DMX data
        dmx_data = bytearray(512)
        for channel, value in channels.items():
            if 1 <= int(channel) <= 512:
                dmx_data[int(channel) - 1] = min(255, max(0, int(value)))
        
        await sender.send_dmx(universe, bytes(dmx_data))
    
    hass.services.async_register(
        DOMAIN, 
        'send_dmx', 
        send_dmx_service,
        schema=vol.Schema({
            vol.Required('universe'): vol.Range(min=1, max=63999),
            vol.Required('channels'): dict,
        })
    )
    
    return True


async def async_setup_entry(hass: HomeAssistant, entry) -> bool:
    """Set up sACN from a config entry"""
    return await async_setup(hass, entry.data)


async def async_unload_entry(hass: HomeAssistant, entry) -> bool:
    """Unload sACN config entry"""
    # Stop sender
    if hass.data[DOMAIN]['sender']:
        await hass.data[DOMAIN]['sender'].stop()
    
    # Stop all receivers
    for receiver in hass.data[DOMAIN]['receivers'].values():
        await receiver.stop()
    
    return True