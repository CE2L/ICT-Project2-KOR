FROM node:20-alpine

WORKDIR /app

RUN addgroup -g 1001 -S nodejs && adduser -S nextjs -u 1001

COPY package.json ./
RUN npm install

COPY --chown=nextjs:nodejs . .

RUN chmod -R 755 node_modules/.bin

RUN npm run build

USER nextjs

EXPOSE 3000

CMD ["npm", "start"]