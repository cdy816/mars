#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/core/runtime:3.1-buster-slim AS base
#FROM mcr.microsoft.com/dotnet/core/sdk:3.1-buster AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443
COPY . .
ENTRYPOINT ["dotnet", "DBInRun.dll"]