FROM python:2.7

# pyinstaller cannot run as root user, so add an user
# -m for creating home directory
RUN pip install pyinstaller && \
    useradd -m -s /bin/bash pyinstaller

# Set the user to use when running a container
USER pyinstaller

WORKDIR /code

# Container will by default run 'pyinstaller --help'
ENTRYPOINT [ "pyinstaller" ]
CMD [ "--help" ]