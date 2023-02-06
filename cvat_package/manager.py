import requests
import glob
import os


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
    def __init__(self, user_name, password):
        super().__init__(user_name)
        self.password = password


class CVATOrganization(CVATObject):
    def __init__(self, org_name, slug):
        super().__init__(org_name)
        self.slug = slug


class CVATProject(CVATObject):
    def __init__(self, project_name, labels):
        super().__init__(project_name)
        self.labels = labels


class CVATTask(CVATObject):
    def __init__(self, task_name, segment_size=100, image_quality=75):
        super().__init__(task_name)
        self.segment_size = segment_size
        self.image_quality = image_quality


class CVATManager:
    def __init__(self, user: CVATUser, organization: CVATOrganization, project: CVATProject):
        self.user = user
        self.organization = organization
        self.project = project

        self.url = 'http://localhost:8080'
        # self.url = 'https://cvat.aibb.ai'
        self.image_dir = 'images'

    def _get_auth_data(self):
        return self.user.name, self.user.password

    def _get_headers(self):
        return {'X-Organization': self.organization.slug}

    def recreate_user(self):
        data = {
            'username': self.user.name,
            'password1': self.user.password,
            'password2': self.user.password
        }

        response = requests.post(f'{self.url}/api/auth/register', json=data)
        assert response.ok or response.text == '{"username":["A user with that username already exists."]}'

        response = requests.get(f'{self.url}/api/users/self', auth=self._get_auth_data())
        assert response.ok

        self.user.id = response.json()['id']

    def _get_organization_id_by_name(self):
        response = requests.get(f'{self.url}/api/organizations', auth=self._get_auth_data())
        assert response.ok

        organizations = [org for org in response.json()['results'] if org['slug'] == self.organization.slug]
        return organizations[0]['id'] if len(organizations) > 0 else None

    def recreate_organization(self):
        self.organization.id = self._get_organization_id_by_name()

        if self.organization.id is not None:
            print('Organization with that slug already exists')
            return

        data = {
            'name': self.organization.name,
            'slug': self.organization.slug,
        }

        response = requests.post(f'{self.url}/api/organizations', json=data, auth=self._get_auth_data())
        assert response.ok

    def _get_project_id_by_name(self):
        response = requests.get(f'{self.url}/api/projects',
                                auth=self._get_auth_data(),
                                headers=self._get_headers())
        assert response.ok

        projects = [project for project in response.json()['results'] if project['name'] == self.project.name]
        return projects[0]['id'] if len(projects) > 0 else None

    def recreate_project(self):
        self.project.id = self._get_project_id_by_name()

        if self.project.id is not None:
            print('Project with that name already exists')
            return

        data = {
            'name': self.project.name,
            'labels': self.project.labels,
            'owner_id': self.user.id
        }

        response = requests.post(f'{self.url}/api/projects',
                                 json=data,
                                 auth=self._get_auth_data(),
                                 headers=self._get_headers())
        assert response.ok
        self.project.id = response.json()['id']

    def get_tasks(self):
        response = requests.get(f'{self.url}/api/tasks',
                                auth=self._get_auth_data(),
                                headers=self._get_headers())
        assert response.ok

        return response.json()['results']

    def clear_tasks(self):
        for task in self.get_tasks():
            response = requests.delete(f'{self.url}/api/tasks/{task["id"]}',
                                       auth=self._get_auth_data(),
                                       headers=self._get_headers())
            assert response.ok

    def recreate_task(self, task: CVATTask):
        data = {
            'name': task.name,
            'project_id': self.project.id,
            'owner_id': self.user.id,
            'segment_size': task.segment_size
        }

        response = requests.post(f'{self.url}/api/tasks',
                                 json=data,
                                 auth=self._get_auth_data(),
                                 headers=self._get_headers())
        assert response.ok

        task.id = response.json()['id']

    def _clear_images(self):
        [os.remove(image) for image in glob.glob(f'{self.image_dir}/*.jpg')]

    def attach_url_images(self, task, images):
        request_files = {}
        self._clear_images()

        for i, url in enumerate(images):
            image_path = f'{self.image_dir}/image_{i}.jpg'

            with open(image_path, 'wb') as file:
                file.write(requests.get(url).content)

            request_files[f'client_files[{i}]'] = open(image_path, 'rb')

        response = requests.post(f'{self.url}/api/tasks/{task.id}/data',
                                 data={'image_quality': task.image_quality},
                                 files=request_files,
                                 auth=self._get_auth_data(),
                                 headers=self._get_headers())
        assert response.ok

    def get_task_jobs(self, task):
        response = requests.get(f'{self.url}/api/tasks/{task.id}/jobs',
                                auth=self._get_auth_data(),
                                headers=self._get_headers())

        assert response.ok
        return response.json()['results']
        # return response.json()

    def get_task_state(self, task):
        jobs = self.get_task_jobs(task)
        states = []

        for job in jobs:
            response = requests.get(f'{self.url}/api/jobs/{job["id"]}',
                                    auth=self._get_auth_data(),
                                    headers=self._get_headers())
            assert response.ok
            states.append(response.json()['state'])

        return all([state == 'completed' for state in states])

    def get_task_annotations(self, task):
        assert self.get_task_state(task)
        jobs = self.get_task_jobs(task)
        annotations = []

        for job in jobs:
            response = requests.get(f'{self.url}/api/jobs/{job["id"]}/annotations',
                                    auth=self._get_auth_data(),
                                    headers=self._get_headers())
            assert response.ok
            annotations.append(response.json())

        return annotations

    def info_about(self):
        response = requests.get(f'{self.url}/api/server/about')
        print(response.text)
