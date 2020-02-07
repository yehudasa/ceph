import datetime
import errno
import json
from functools import wraps

from ceph.deployment.inventory import Device
from prettytable import PrettyTable

from mgr_util import format_bytes, to_pretty_timedelta

try:
    from typing import List, Set, Optional
except ImportError:
    pass  # just for type checking.


from ceph.deployment.drive_group import DriveGroupSpec, DriveGroupValidationError, \
    DeviceSelection
from mgr_module import MgrModule, CLICommand, HandleCommandResult

import orchestrator


class OrchestratorCli(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS = [
        {
            'name': 'orchestrator',
            'type': 'str',
            'default': None,
            'desc': 'Orchestrator backend',
            'enum_allowed': ['cephadm', 'rook', 'ansible', 'deepsea',
                             'test_orchestrator'],
            'runtime': True,
        },
    ]
    NATIVE_OPTIONS = []  # type: List[dict]

    def __init__(self, *args, **kwargs):
        super(OrchestratorCli, self).__init__(*args, **kwargs)
        self.ident = set()  # type: Set[str]
        self.fault = set()  # type: Set[str]
        self._load()
        self._refresh_health()

    def _load(self):
        active = self.get_store('active_devices')
        if active:
            decoded = json.loads(active)
            self.ident = set(decoded.get('ident', []))
            self.fault = set(decoded.get('fault', []))
        self.log.debug('ident {}, fault {}'.format(self.ident, self.fault))

    def _save(self):
        encoded = json.dumps({
            'ident': list(self.ident),
            'fault': list(self.fault),
            })
        self.set_store('active_devices', encoded)

    def _refresh_health(self):
        h = {}
        if self.ident:
            h['DEVICE_IDENT_ON'] = {
                'severity': 'warning',
                'summary': '%d devices have ident light turned on' % len(
                    self.ident),
                'detail': ['{} ident light enabled'.format(d) for d in self.ident]
            }
        if self.fault:
            h['DEVICE_FAULT_ON'] = {
                'severity': 'warning',
                'summary': '%d devices have fault light turned on' % len(
                    self.fault),
                'detail': ['{} fault light enabled'.format(d) for d in self.ident]
            }
        self.set_health_checks(h)

    def _get_device_locations(self, dev_id):
        # type: (str) -> List[orchestrator.DeviceLightLoc]
        locs = [d['location'] for d in self.get('devices')['devices'] if d['devid'] == dev_id]
        return [orchestrator.DeviceLightLoc(**l) for l in sum(locs, [])]

    @orchestrator._cli_read_command(
        prefix='device ls-lights',
        desc='List currently active device indicator lights')
    def _device_ls(self):
        return HandleCommandResult(
            stdout=json.dumps({
                'ident': list(self.ident),
                'fault': list(self.fault)
                }, indent=4, sort_keys=True))

    def light_on(self, fault_ident, devid):
        # type: (str, str) -> HandleCommandResult
        assert fault_ident in ("fault", "ident")
        locs = self._get_device_locations(devid)
        if locs is None:
            return HandleCommandResult(stderr='device {} not found'.format(devid),
                                       retval=-errno.ENOENT)

        getattr(self, fault_ident).add(devid)
        self._save()
        self._refresh_health()
        completion = self.blink_device_light(fault_ident, True, locs)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=str(completion.result))

    def light_off(self, fault_ident, devid, force):
        # type: (str, str, bool) -> HandleCommandResult
        assert fault_ident in ("fault", "ident")
        locs = self._get_device_locations(devid)
        if locs is None:
            return HandleCommandResult(stderr='device {} not found'.format(devid),
                                       retval=-errno.ENOENT)

        try:
            completion = self.blink_device_light(fault_ident, False, locs)
            self._orchestrator_wait([completion])

            if devid in getattr(self, fault_ident):
                getattr(self, fault_ident).remove(devid)
                self._save()
                self._refresh_health()
            return HandleCommandResult(stdout=str(completion.result))

        except:
            # There are several reasons the try: block might fail:
            # 1. the device no longer exist
            # 2. the device is no longer known to Ceph
            # 3. the host is not reachable
            if force and devid in getattr(self, fault_ident):
                getattr(self, fault_ident).remove(devid)
                self._save()
                self._refresh_health()
            raise

    @orchestrator._cli_write_command(
        prefix='device light',
        cmd_args='name=enable,type=CephChoices,strings=on|off '
                 'name=devid,type=CephString '
                 'name=light_type,type=CephChoices,strings=ident|fault,req=false '
                 'name=force,type=CephBool,req=false',
        desc='Enable or disable the device light. Default type is `ident`\n'
             'Usage: device light (on|off) <devid> [ident|fault] [--force]')
    def _device_light(self, enable, devid, light_type=None, force=False):
        # type: (str, str, Optional[str], bool) -> HandleCommandResult
        light_type = light_type or 'ident'
        on = enable == 'on'
        if on:
            return self.light_on(light_type, devid)
        else:
            return self.light_off(light_type, devid, force)

    def _select_orchestrator(self):
        return self.get_module_option("orchestrator")

    @orchestrator._cli_write_command(
        'orchestrator host add',
        "name=host,type=CephString,req=true",
        'Add a host')
    def _add_host(self, host):
        completion = self.add_host(host)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator host rm',
        "name=host,type=CephString,req=true",
        'Remove a host')
    def _remove_host(self, host):
        completion = self.remove_host(host)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_read_command(
        'orchestrator host ls',
        'name=format,type=CephChoices,strings=json|plain,req=false',
        'List hosts')
    def _get_hosts(self, format='plain'):
        completion = self.get_hosts()
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        if format == 'json':
            hosts = [dict(host=node.name, labels=node.labels)
                     for node in completion.result]
            output = json.dumps(hosts, sort_keys=True)
        else:
            table = PrettyTable(
                ['HOST', 'LABELS'],
                border=False)
            table.align = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 1
            for node in completion.result:
                table.add_row((node.name, ' '.join(node.labels)))
            output = table.get_string()
        return HandleCommandResult(stdout=output)

    @orchestrator._cli_write_command(
        'orchestrator host label add',
        'name=host,type=CephString '
        'name=label,type=CephString',
        'Add a host label')
    def _host_label_add(self, host, label):
        completion = self.add_host_label(host, label)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator host label rm',
        'name=host,type=CephString '
        'name=label,type=CephString',
        'Add a host label')
    def _host_label_rm(self, host, label):
        completion = self.remove_host_label(host, label)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_read_command(
        'orchestrator device ls',
        "name=host,type=CephString,n=N,req=false "
        "name=format,type=CephChoices,strings=json|plain,req=false "
        "name=refresh,type=CephBool,req=false",
        'List devices on a node')
    def _list_devices(self, host=None, format='plain', refresh=False):
        # type: (Optional[List[str]], str, bool) -> HandleCommandResult
        """
        Provide information about storage devices present in cluster hosts

        Note: this does not have to be completely synchronous. Slightly out of
        date hardware inventory is fine as long as hardware ultimately appears
        in the output of this command.
        """
        nf = orchestrator.InventoryFilter(nodes=host) if host else None

        completion = self.get_inventory(node_filter=nf, refresh=refresh)

        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)

        if format == 'json':
            data = [n.to_json() for n in completion.result]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            out = []

            table = PrettyTable(
                ['HOST', 'PATH', 'TYPE', 'SIZE', 'DEVICE', 'AVAIL',
                 'REJECT REASONS'],
                border=False)
            table.align = 'l'
            table._align['SIZE'] = 'r'
            table.left_padding_width = 0
            table.right_padding_width = 1
            for host_ in completion.result: # type: orchestrator.InventoryNode
                for d in host_.devices.devices:  # type: Device
                    table.add_row(
                        (
                            host_.name,
                            d.path,
                            d.human_readable_type,
                            format_bytes(d.sys_api.get('size', 0), 5),
                            d.device_id,
                            d.available,
                            ', '.join(d.rejected_reasons)
                        )
                    )
            out.append(table.get_string())
            return HandleCommandResult(stdout='\n'.join(out))

    @orchestrator._cli_read_command(
        'orchestrator service ls',
        "name=host,type=CephString,req=false "
        "name=svc_type,type=CephChoices,strings=mon|mgr|osd|mds|iscsi|nfs|rgw|rbd-mirror,req=false "
        "name=svc_id,type=CephString,req=false "
        "name=format,type=CephChoices,strings=json|plain,req=false "
        "name=refresh,type=CephBool,req=false",
        'List services known to orchestrator')
    def _list_services(self, host=None, svc_type=None, svc_id=None, format='plain', refresh=False):
        # XXX this is kind of confusing for people because in the orchestrator
        # context the service ID for MDS is the filesystem ID, not the daemon ID

        completion = self.describe_service(svc_type, svc_id, host, refresh=refresh)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        services = completion.result

        def ukn(s):
            return '<unknown>' if s is None else s
        # Sort the list for display
        services.sort(key=lambda s: (ukn(s.service_type), ukn(s.nodename), ukn(s.service_instance)))

        if len(services) == 0:
            return HandleCommandResult(stdout="No services reported")
        elif format == 'json':
            data = [s.to_json() for s in services]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            now = datetime.datetime.utcnow()
            table = PrettyTable(
                ['NAME', 'HOST', 'STATUS', 'REFRESHED',
                 'VERSION', 'IMAGE NAME', 'IMAGE ID', 'CONTAINER ID'],
                border=False)
            table.align = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 1
            for s in sorted(services, key=lambda s: s.name()):
                status = {
                    -1: 'error',
                    0: 'stopped',
                    1: 'running',
                    None: '<unknown>'
                }[s.status]

                if s.last_refresh:
                    age = to_pretty_timedelta(now - s.last_refresh) + ' ago'
                else:
                    age = '-'
                table.add_row((
                    s.name(),
                    ukn(s.nodename),
                    status,
                    age,
                    ukn(s.version),
                    ukn(s.container_image_name),
                    ukn(s.container_image_id)[0:12],
                    ukn(s.container_id)[0:12]))

            return HandleCommandResult(stdout=table.get_string())

    @orchestrator._cli_write_command(
        'orchestrator osd create',
        "name=svc_arg,type=CephString,req=false",
        'Create an OSD service. Either --svc_arg=host:drives or -i <drive_group>')
    def _create_osd(self, svc_arg=None, inbuf=None):
        # type: (Optional[str], Optional[str]) -> HandleCommandResult
        """Create one or more OSDs"""

        usage = """
Usage:
  ceph orchestrator osd create -i <json_file>
  ceph orchestrator osd create host:device1,device2,...
"""

        if inbuf:
            try:
                drive_group = DriveGroupSpec.from_json(json.loads(inbuf))
            except ValueError as e:
                msg = 'Failed to read JSON input: {}'.format(str(e)) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

        elif svc_arg:
            try:
                node_name, block_device = svc_arg.split(":")
                block_devices = block_device.split(',')
            except (TypeError, KeyError, ValueError):
                msg = "Invalid host:device spec: '{}'".format(svc_arg) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

            devs = DeviceSelection(paths=block_devices)
            drive_group = DriveGroupSpec(node_name, data_devices=devs)
        else:
            return HandleCommandResult(-errno.EINVAL, stderr=usage)

        completion = self.create_osds(drive_group)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        self.log.warning(str(completion.result))
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator osd rm',
        "name=svc_id,type=CephString,n=N",
        'Remove OSD services')
    def _osd_rm(self, svc_id):
        # type: (List[str]) -> HandleCommandResult
        """
        Remove OSD's
        :cmd : Arguments for remove the osd
        """
        completion = self.remove_osds(svc_id)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator rbd-mirror add',
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false",
        'Create an rbd-mirror service')
    def _rbd_mirror_add(self, num=None, hosts=None):
        spec = orchestrator.StatelessServiceSpec(
            None,
            placement=orchestrator.PlacementSpec(hosts=hosts, count=num))
        completion = self.add_rbd_mirror(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator rbd-mirror update',
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false "
        "name=label,type=CephString,req=false",
        'Update the number of rbd-mirror instances')
    def _rbd_mirror_update(self, num, label=None, hosts=[]):
        spec = orchestrator.StatelessServiceSpec(
            None,
            placement=orchestrator.PlacementSpec(hosts=hosts, count=num, label=label))
        completion = self.update_rbd_mirror(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator rbd-mirror rm',
        "name=name,type=CephString,req=false",
        'Remove rbd-mirror service or rbd-mirror service instance')
    def _rbd_mirror_rm(self, name=None):
        completion = self.remove_rbd_mirror(name)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator mds add',
        "name=fs_name,type=CephString "
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false",
        'Create an MDS service')
    def _mds_add(self, fs_name, num=None, hosts=None):
        spec = orchestrator.StatelessServiceSpec(
            fs_name,
            placement=orchestrator.PlacementSpec(hosts=hosts, count=num))
        completion = self.add_mds(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator mds update',
        "name=fs_name,type=CephString "
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false "
        "name=label,type=CephString,req=false",
        'Update the number of MDS instances for the given fs_name')
    def _mds_update(self, fs_name, num=None, label=None, hosts=[]):
        placement = orchestrator.PlacementSpec(label=label, count=num, hosts=hosts)
        placement.validate()

        spec = orchestrator.StatelessServiceSpec(
            fs_name,
            placement=placement)

        completion = self.update_mds(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator mds rm',
        "name=name,type=CephString",
        'Remove an MDS service (mds id or fs_name)')
    def _mds_rm(self, name):
        completion = self.remove_mds(name)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator rgw add',
        'name=realm_name,type=CephString '
        'name=zone_name,type=CephString '
        'name=num,type=CephInt,req=false '
        "name=hosts,type=CephString,n=N,req=false",
        'Create an RGW service. A complete <rgw_spec> can be provided'\
        ' using <-i> to customize completelly the RGW service')
    def _rgw_add(self, realm_name, zone_name, num=1, hosts=None, inbuf=None):
        usage = """
Usage:
  ceph orchestrator rgw add -i <json_file>
  ceph orchestrator rgw add <realm_name> <zone_name>
        """
        if inbuf:
            try:
                rgw_spec = orchestrator.RGWSpec.from_json(json.loads(inbuf))
            except ValueError as e:
                msg = 'Failed to read JSON input: {}'.format(str(e)) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)
        rgw_spec = orchestrator.RGWSpec(
            rgw_realm=realm_name,
            rgw_zone=zone_name,
            placement=orchestrator.PlacementSpec(hosts=hosts, count=num))

        completion = self.add_rgw(rgw_spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator rgw update',
        'name=realm_name,type=CephString '
        'name=zone_name,type=CephString '
        'name=num,type=CephInt,req=false '
        'name=hosts,type=CephString,n=N,req=false '
        'name=label,type=CephString,req=false',
        'Update the number of RGW instances for the given zone')
    def _rgw_update(self, zone_name, realm_name, num=None, label=None, hosts=[]):
        spec = orchestrator.RGWSpec(
            rgw_realm=realm_name,
            rgw_zone=zone_name,
            placement=orchestrator.PlacementSpec(hosts=hosts, label=label, count=num))
        completion = self.update_rgw(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator rgw rm',
        'name=realm_name,type=CephString '
        'name=zone_name,type=CephString',
        'Remove an RGW service')
    def _rgw_rm(self, realm_name, zone_name):
        name = realm_name + '.' + zone_name
        completion = self.remove_rgw(name)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator nfs add',
        "name=svc_arg,type=CephString "
        "name=pool,type=CephString "
        "name=namespace,type=CephString,req=false "
        'name=num,type=CephInt,req=false '
        'name=hosts,type=CephString,n=N,req=false '
        'name=label,type=CephString,req=false',
        'Create an NFS service')
    def _nfs_add(self, svc_arg, pool, namespace=None, num=None, label=None, hosts=[]):
        spec = orchestrator.NFSServiceSpec(
            svc_arg,
            pool=pool,
            namespace=namespace,
            placement=orchestrator.PlacementSpec(label=label, hosts=hosts, count=num),
        )
        spec.validate_add()
        completion = self.add_nfs(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator nfs update',
        "name=svc_id,type=CephString "
        'name=num,type=CephInt,req=false '
        'name=hosts,type=CephString,n=N,req=false '
        'name=label,type=CephString,req=false',
        'Scale an NFS service')
    def _nfs_update(self, svc_id, num=None, label=None, hosts=[]):
        # type: (str, Optional[int], Optional[str], List[str]) -> HandleCommandResult
        spec = orchestrator.NFSServiceSpec(
            svc_id,
            placement=orchestrator.PlacementSpec(label=label, hosts=hosts, count=num),
        )
        completion = self.update_nfs(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator nfs rm',
        "name=svc_id,type=CephString",
        'Remove an NFS service')
    def _nfs_rm(self, svc_id):
        completion = self.remove_nfs(svc_id)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator service',
        "name=action,type=CephChoices,strings=start|stop|restart|redeploy|reconfig "
        "name=svc_type,type=CephString "
        "name=svc_name,type=CephString",
        'Start, stop, restart, redeploy, or reconfig an entire service (i.e. all daemons)')
    def _service_action(self, action, svc_type, svc_name):
        completion = self.service_action(action, svc_type, service_name=svc_name)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator service-instance',
        "name=action,type=CephChoices,strings=start|stop|restart|redeploy|reconfig "
        "name=svc_type,type=CephString "
        "name=svc_id,type=CephString",
        'Start, stop, restart, redeploy, or reconfig a specific service instance')
    def _service_instance_action(self, action, svc_type, svc_id):
        completion = self.service_action(action, svc_type, service_id=svc_id)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator mgr update',
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false "
        "name=label,type=CephString,req=false",
        'Update the number of manager instances')
    def _update_mgrs(self, num=None, hosts=[], label=None):

        placement = orchestrator.PlacementSpec(label=label, count=num, hosts=hosts)
        placement.validate()

        spec = orchestrator.StatefulServiceSpec(placement=placement)

        completion = self.update_mgrs(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator mon update',
        "name=num,type=CephInt,req=false "
        "name=hosts,type=CephString,n=N,req=false "
        "name=label,type=CephString,req=false",
        'Update the number of monitor instances')
    def _update_mons(self, num=None, hosts=[], label=None):
        if not num and not hosts and not label:
            # Improve Error message. Point to parse_host_spec examples
            raise orchestrator.OrchestratorValidationError("Mons need a placement spec. (num, host, network, name(opt))")
        placement = orchestrator.PlacementSpec(label=label, count=num, hosts=hosts)
        placement.validate()

        spec = orchestrator.StatefulServiceSpec(placement=placement)

        completion = self.update_mons(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'orchestrator set backend',
        "name=module_name,type=CephString,req=true",
        'Select orchestrator module backend')
    def _set_backend(self, module_name):
        """
        We implement a setter command instead of just having the user
        modify the setting directly, so that we can validate they're setting
        it to a module that really exists and is enabled.

        There isn't a mechanism for ensuring they don't *disable* the module
        later, but this is better than nothing.
        """
        mgr_map = self.get("mgr_map")

        if module_name is None or module_name == "":
            self.set_module_option("orchestrator", None)
            return HandleCommandResult()

        for module in mgr_map['available_modules']:
            if module['name'] != module_name:
                continue

            if not module['can_run']:
                continue

            enabled = module['name'] in mgr_map['modules']
            if not enabled:
                return HandleCommandResult(-errno.EINVAL,
                                           stderr="Module '{module_name}' is not enabled. \n Run "
                                                  "`ceph mgr module enable {module_name}` "
                                                  "to enable.".format(module_name=module_name))

            try:
                is_orchestrator = self.remote(module_name,
                                              "is_orchestrator_module")
            except NameError:
                is_orchestrator = False

            if not is_orchestrator:
                return HandleCommandResult(-errno.EINVAL,
                                           stderr="'{0}' is not an orchestrator module".format(module_name))

            self.set_module_option("orchestrator", module_name)

            return HandleCommandResult()

        return HandleCommandResult(-errno.EINVAL, stderr="Module '{0}' not found".format(module_name))

    @orchestrator._cli_write_command(
        'orchestrator cancel',
        desc='cancels ongoing operations')
    def _cancel(self):
        """
        ProgressReferences might get stuck. Let's unstuck them.
        """
        self.cancel_completions()
        return HandleCommandResult()

    @orchestrator._cli_read_command(
        'orchestrator status',
        desc='Report configured backend and its status')
    def _status(self):
        o = self._select_orchestrator()
        if o is None:
            raise orchestrator.NoOrchestrator()

        avail, why = self.available()
        if avail is None:
            # The module does not report its availability
            return HandleCommandResult(stdout="Backend: {0}".format(o))
        else:
            return HandleCommandResult(stdout="Backend: {0}\nAvailable: {1}{2}".format(
                                           o, avail,
                                           " ({0})".format(why) if not avail else ""
                                       ))

    def self_test(self):
        old_orch = self._select_orchestrator()
        self._set_backend('')
        assert self._select_orchestrator() is None
        self._set_backend(old_orch)

        e1 = self.remote('selftest', 'remote_from_orchestrator_cli_self_test', "ZeroDivisionError")
        try:
            orchestrator.raise_if_exception(e1)
            assert False
        except ZeroDivisionError as e:
            assert e.args == ('hello', 'world')

        e2 = self.remote('selftest', 'remote_from_orchestrator_cli_self_test', "OrchestratorError")
        try:
            orchestrator.raise_if_exception(e2)
            assert False
        except orchestrator.OrchestratorError as e:
            assert e.args == ('hello', 'world')

        c = orchestrator.TrivialReadCompletion(result=True)
        assert c.has_result

    @orchestrator._cli_write_command(
        'upgrade check',
        'name=image,type=CephString,req=false '
        'name=ceph_version,type=CephString,req=false',
        desc='Check service versions vs available and target containers')
    def _upgrade_check(self, image=None, ceph_version=None):
        completion = self.upgrade_check(image=image, version=ceph_version)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'upgrade status',
        desc='Check service versions vs available and target containers')
    def _upgrade_status(self):
        completion = self.upgrade_status()
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        r = {
            'target_image': completion.result.target_image,
            'in_progress': completion.result.in_progress,
            'services_complete': completion.result.services_complete,
            'message': completion.result.message,
        }
        out = json.dumps(r, indent=4)
        return HandleCommandResult(stdout=out)

    @orchestrator._cli_write_command(
        'upgrade start',
        'name=image,type=CephString,req=false '
        'name=ceph_version,type=CephString,req=false',
        desc='Initiate upgrade')
    def _upgrade_start(self, image=None, ceph_version=None):
        completion = self.upgrade_start(image, ceph_version)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'upgrade pause',
        desc='Pause an in-progress upgrade')
    def _upgrade_pause(self):
        completion = self.upgrade_pause()
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'upgrade resume',
        desc='Resume paused upgrade')
    def _upgrade_resume(self):
        completion = self.upgrade_resume()
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @orchestrator._cli_write_command(
        'upgrade stop',
        desc='Stop an in-progress upgrade')
    def _upgrade_stop(self):
        completion = self.upgrade_stop()
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())
