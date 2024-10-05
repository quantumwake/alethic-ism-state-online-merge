# Start from the latest golang base image
FROM golang:1.22

# Set up environment variables
ARG GIT_USERNAME
ARG GIT_TOKEN

# Create a .netrc file to store Git credentials
RUN echo -e "machine github.com\nlogin $GIT_USERNAME\npassword $GIT_TOKEN" > ~/.netrc

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app
RUN go build -o main .

# Expose port 8080 to the outside world
EXPOSE 8080

# Command to run the executable
CMD ["./main"]