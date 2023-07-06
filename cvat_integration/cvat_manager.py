from urllib.parse import unquote, urlparse
import asyncio
import websockets
import websockets.exceptions
import threading
import base64
import requests
import zipfile
import tempfile
import logging
import time
import json
import enum
import os
import io


from settings import Settings
settings = Settings.get_instance()


class NotificationThread(threading.Thread):
    server_url = settings.notification_server_url
    logger = logging.getLogger(__name__)
    active = False

    class MessageType(enum.Enum):
        handshake = 'handshake'
        webhook = 'webhook'
        ping = 'ping'

    def __init__(self):
        super(NotificationThread, self).__init__()
        self.connections = set()

    def run(self):
        async def handler(socket):
            self.connections.add(socket)

            while True:
                try:
                    message = await socket.recv()

                    message_json = NotificationThread.unpack_message(message)
                    assert isinstance(message_json, dict) and 'message_type' in message_json

                    if message_json['message_type'] == NotificationThread.MessageType.webhook.value:
                        await asyncio.gather(*[connection.send(message) for connection in self.connections],
                                             return_exceptions=True)

                except websockets.exceptions.ConnectionClosedOK:
                    self.connections.discard(socket)
                    break

        async def wrapper():
            components = urlparse(NotificationThread.server_url)
            async with websockets.serve(handler,
                                        components.hostname,
                                        components.port):
                await asyncio.Future()

        if not NotificationThread.active:
            NotificationThread.active = True
            asyncio.run(wrapper())
        else:
            NotificationThread.logger.info('Notification server already launched')

    @staticmethod
    def send_message(message):
        async def connect():
            async with websockets.connect(NotificationThread.server_url) as socket:
                await socket.send(message)

        asyncio.run(connect())

    @staticmethod
    def pack_message(message_type, message_json):
        return json.dumps({
            'message_type': message_type,
            'message_json': message_json
        })

    @staticmethod
    def unpack_message(message):
        return json.loads(message)


class CVATObject:
    def __init__(self, object_name, object_id=-1):
        self.name = object_name
        self._id = object_id

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, object_id):
        self._id = object_id


class CVATUser(CVATObject):
    def __init__(self, username, email, password):
        super().__init__(username)
        self.email = email
        self.password = password

    @staticmethod
    def create_from_basic_auth(basic_auth):
        class DecodeError(Exception):
            pass

        try:
            username, email, password = unquote(base64.b64decode(basic_auth).decode()).split(':', 2)
        except Exception:
            raise DecodeError

        return CVATUser(username, email, password)


class CVATOrganization(CVATObject):
    def __init__(self, org_name, slug):
        super().__init__(org_name)
        self.slug = slug


class CVATProject(CVATObject):
    def __init__(self, project_name, categories):
        super().__init__(project_name)
        self.categories = categories


class CVATTask(CVATObject):
    def __init__(self, task_name, segment_size=100, image_quality=75):
        super().__init__(task_name)
        self.segment_size = segment_size
        self.image_quality = image_quality


class CVATManager:
    url = settings.cvat_api_base_url
    organization_name_suffix = 'organization'
    organization_slug_suffix = 'org'
    logger = logging.getLogger(__name__)

    def __init__(self, user: CVATUser, organization: CVATOrganization, project: CVATProject):
        self.user = user
        self.organization = organization
        self.project = project

    def _get_auth_data(self):
        return self.user.name, self.user.password

    def _get_headers(self):
        return {'X-Organization': self.organization.slug}

    def _get_user_id_by_name(self):
        response = requests.get(f'{CVATManager.url}/api/users/self', auth=self._get_auth_data())
        assert response.ok, response.text

        return response.json()['id']

    def recreate_user(self):
        data = {
            'username': self.user.name,
            'email': self.user.email,
            'password1': self.user.password,
            'password2': self.user.password
        }

        response = requests.post(f'{CVATManager.url}/api/auth/register', json=data)
        assert response.ok or 'A user with that username already exists' in response.text, response.text

        self.user.id = self._get_user_id_by_name()
        return self

    def _get_organization_id_by_name(self):
        response = requests.get(f'{CVATManager.url}/api/organizations', auth=self._get_auth_data())
        assert response.ok, response.text

        organizations = [org for org in response.json() if org['slug'] == self.organization.slug]
        return organizations[0]['id'] if len(organizations) > 0 else None

    def recreate_organization(self):
        self.organization.id = self._get_organization_id_by_name()

        if self.organization.id is not None:
            CVATManager.logger.info('Organization with that slug already exists')
            return self

        data = {
            'name': self.organization.name,
            'slug': self.organization.slug,
        }

        response = requests.post(f'{CVATManager.url}/api/organizations', json=data, auth=self._get_auth_data())
        assert response.ok, response.text

        return self

    def _get_project_id_by_name(self):
        response = requests.get(f'{CVATManager.url}/api/projects',
                                auth=self._get_auth_data(),
                                headers=self._get_headers())
        assert response.ok, response.text

        projects = [project for project in response.json()['results'] if project['name'] == self.project.name]
        return projects[0]['id'] if len(projects) > 0 else None

    def recreate_project(self):
        self.project.id = self._get_project_id_by_name()

        if self.project.id is not None:
            CVATManager.logger.info('Project with that name already exists')
            return self

        data = {
            'name': self.project.name,
            'labels': self.project.categories,
            'owner_id': self.user.id
        }

        response = requests.post(f'{CVATManager.url}/api/projects',
                                 json=data,
                                 auth=self._get_auth_data(),
                                 headers=self._get_headers())
        assert response.ok, response.text
        self.project.id = response.json()['id']

        return self

    def get_tasks(self):
        response = requests.get(f'{CVATManager.url}/api/tasks',
                                auth=self._get_auth_data(),
                                headers=self._get_headers())
        assert response.ok, response.text
        return [task for task in response.json()['results'] if task['project_id'] == self.project.id]

    def clear_tasks(self):
        for task in self.get_tasks():
            response = requests.delete(f'{CVATManager.url}/api/tasks/{task["id"]}',
                                       auth=self._get_auth_data(),
                                       headers=self._get_headers())
            assert response.ok, response.text

    def recreate_task(self, task: CVATTask):
        data = {
            'name': task.name,
            'project_id': self.project.id,
            'owner_id': self.user.id,
            'segment_size': task.segment_size
        }

        response = requests.post(f'{CVATManager.url}/api/tasks',
                                 json=data,
                                 auth=self._get_auth_data(),
                                 headers=self._get_headers())
        assert response.ok, response.text
        task.id = response.json()['id']

        return response.json()

    def attach_images(self, task, images, filenames):
        request_files = {}

        with tempfile.TemporaryDirectory() as directory:
            for i, image in enumerate(images):
                image_path = f'{directory}/{filenames[i]}'

                with open(image_path, 'wb') as file:
                    file.write(base64.b64decode(image))

                request_files[f'client_files[{i}]'] = open(image_path, 'rb')

            response = requests.post(f'{CVATManager.url}/api/tasks/{task.id}/data',
                                     data={'image_quality': task.image_quality},
                                     files=request_files,
                                     auth=self._get_auth_data(),
                                     headers=self._get_headers())

            # releasing files for tempfile to delete it
            del request_files
        assert response.ok, response.text

    def get_task_status(self, task):
        response = requests.get(f'{CVATManager.url}/api/tasks/{task.id}',
                                auth=self._get_auth_data(),
                                headers=self._get_headers())

        assert response.ok, response.text
        return response.json()

    def get_task_annotations(self, task, annotation_format='PASCAL VOC 1.1'):
        # segments???
        # on 2.3.0 version CVAT has json response that differs from CVAT REST API documentation
        jobs = [segment['jobs'][0] for segment in self.get_task_status(task)['segments']]
        annotations = []

        directory = tempfile.TemporaryDirectory()

        for job in jobs:
            response = requests.get(f'{CVATManager.url}/api/jobs/{job["id"]}/annotations',
                                    params={'format': annotation_format},
                                    auth=self._get_auth_data(),
                                    headers=self._get_headers())
            assert response.status_code == 202, response.text

            while response.status_code != 201:
                response = requests.get(f'{CVATManager.url}/api/jobs/{job["id"]}/annotations',
                                        params={'format': annotation_format},
                                        auth=self._get_auth_data(),
                                        headers=self._get_headers())
                time.sleep(0.1)
            assert response.status_code == 201, response.text

            response = requests.get(f'{CVATManager.url}/api/jobs/{job["id"]}/annotations',
                                    params={'format': annotation_format, 'action': 'download'},
                                    auth=self._get_auth_data(),
                                    headers=self._get_headers(),
                                    stream=True)
            assert response.ok, response.text

            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                voc_annotations = [path for path in archive.namelist() if 'Annotations' in path and '.xml' in path]
                coco_annotations = [path for path in archive.namelist() if 'annotations' in path and '.json' in path]
                archive.extractall(directory.name)

                annotations += [os.path.join(directory.name, "Annotations", os.path.basename(path))
                                for path in voc_annotations]
                annotations += [os.path.join(directory.name, "Annotations", os.path.basename(path))
                                for path in coco_annotations]

        return directory, annotations

    def get_webhooks(self):
        response = requests.get(f'{CVATManager.url}/api/webhooks',
                                auth=self._get_auth_data(),
                                headers=self._get_headers())
        
        assert response.ok, response.text
        return [webhook for webhook in response.json()['results'] if webhook['project'] == self.project.id]

    def recreate_webhook(self, target_url):
        webhooks = self.get_webhooks()
        
        if len(webhooks):
            CVATManager.logger.info('Webhook already exists')
            return self

        data = {
            'target_url': target_url,
            'type': 'project',
            'project_id': self.project.id,
            'events': ['update:job']
        }

        response = requests.post(f'{CVATManager.url}/api/webhooks',
                                 json=data,
                                 auth=self._get_auth_data(),
                                 headers=self._get_headers())
        
        assert response.status_code == 201, response.text
        return self

    @staticmethod
    def launch_notification_server():
        thread = NotificationThread()
        thread.daemon = True
        thread.start()

    @staticmethod
    def create_CVAT_manager(basic_auth, categories, project_id):
        user = CVATUser.create_from_basic_auth(basic_auth)

        organization_name = f'{user.name}-{CVATManager.organization_name_suffix}'
        organization_slug = f'{user.name[:6]}-{CVATManager.organization_slug_suffix}'

        organization = CVATOrganization(organization_name, organization_slug)
        project = CVATProject(project_id, categories)

        return CVATManager(user, organization, project)

    @staticmethod
    def init_CVAT_entities(basic_auth, categories, project_id, webhook_target_url):
        CVATManager.launch_notification_server()
        return CVATManager.create_CVAT_manager(basic_auth, categories, project_id)\
            .recreate_user()\
            .recreate_organization()\
            .recreate_project()\
            .recreate_webhook(webhook_target_url)

    @staticmethod
    def get_CVAT_entities(basic_auth, project_id):
        manager = CVATManager.create_CVAT_manager(basic_auth, None, project_id)

        manager.user.id = manager._get_user_id_by_name()
        manager.organization.id = manager._get_organization_id_by_name()
        manager.project.id = manager._get_project_id_by_name()

        return manager

    @staticmethod
    def process_webhook(event, job, sender, project_id):
        assert 'username' in sender and 'id' in sender

        if event == 'ping':
            webhook_info = {
                'username': sender['username'],
                'user_id': sender['id'],
                'task_id': 0,
                'project_id': project_id
                
            }
            message = NotificationThread.pack_message(
                NotificationThread.MessageType.ping.value,
                webhook_info
             )
            NotificationThread.send_message(message)

        else:
            if job['status'] == 'completed':
                                                 
                webhook_info = {
                 'username': sender['username'],
                 'user_id': sender['id'],
                 'task_id': job['task_id'],
                 'project_id': project_id
                }

                message = NotificationThread.pack_message(
                    NotificationThread.MessageType.webhook.value,
                    webhook_info
                 )
                NotificationThread.send_message(message)
