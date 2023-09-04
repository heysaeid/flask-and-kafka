from setuptools import setup


with open("README.md", "r") as f:
    long_description = f.read()


setup(
    name="flask-and-kafka",
    version='0.0.3',
    url="https://github.com/heysaeid/flask-and-kafka.git",
    license="MIT",
    author="Saeid Noormohammadi",
    author_email="heysaeid92@gmail.com",
    description="Easily write your kafka producers and consumers in flask",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=[
        "flask", 
        "kafka", 
        "flask kafka", 
        "flask kafka consumer", 
        "flask-kafka", 
        "flask-kafka-consumer", 
        "flask-and-kafka", 
        "Flask-And-Kafka", 
        "flask kafka producer",
        "kafka producer",
        "flask-kafka-producer"
    ],
    packages=["flask_and_kafka"],
    zip_safe=False,
    platforms="any",
    install_requires=[
        "flask",
        "confluent-kafka",
    ],
    classifiers=[
        "Framework :: Flask",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
