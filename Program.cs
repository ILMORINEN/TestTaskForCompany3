// Дополнительная информация
// 1. Получение данных с сайта
//    Данные забираются группами по 100 000 записей. При получении группы в 1000 элементов время получения одного элемента 0.9 мс.
//    При 10 000 - 0.35 мс. При 100 000 - 0.34 мс.
// 2. Валидация
//    Перед сохранением в базу каждая запись проверяется на соответствие. Номер декларации имеет 28 символов. ИНН продавца и покупателя 10 или 12 цифр.
//    Объемы древесины должны быть больше нуля. Дата сделки обязательна.

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace TestTaskForCompany3
{
    class Program
    {
        const int PageSize = 100000;
        static void Main(string[] args)
        {
            while (true)
            {
                var deals = new List<Deal>();

                try
                {
                    // Получаем количество страниц заданного размера на сайте
                    int pageCount = GetDealsCountFromSiteAsync().Result / PageSize + 1;
                    Stopwatch sw = Stopwatch.StartNew();
                    for (int i = 0; i < pageCount; i++)
                    {
                        // Получаем записи на сайте
                        deals.AddRange(GetDealsFromServerAsync(PageSize, i).Result);
                    }
                    SaveDealsToDatabase(deals, @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=TestTaskForCompany3;Integrated Security=True;");
                    sw.Stop();
                    Console.WriteLine(sw.ElapsedMilliseconds / 1000 / 60);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{DateTime.Now} Ошибка: {ex.Message}");
                    if (ex.InnerException != null)
                        Console.WriteLine(ex.InnerException.Message);
                }
                // Сохраняем записи в БД

                Console.WriteLine($"{DateTime.Now} Данные с сайта были получены и загружены в базу.");

                Thread.Sleep(10 * 60 * 1000);
            }
        }

        private static async Task<int> GetDealsCountFromSiteAsync()
        {
            using (HttpClient client = new HttpClient())
            {
                // Задаем url для запросов
                const string RequestUrl = "https://www.lesegais.ru/open-area/graphql";

                // Устанавливаем заголовки
                client.DefaultRequestHeaders.Add("Host", "www.lesegais.ru");
                client.DefaultRequestHeaders.Add("User-Agent", "Chrome/108.0.0.0");

                // Устанавливаем заполнение тела запроса
                string jsonBody =
                @"{
                ""query"": ""query SearchReportWoodDealCount($size: Int!, $number: Int!, $filter: Filter, $orders: [Order!]) {\n searchReportWoodDeal(filter: $filter, pageable: { number: $number, size: $size}, orders: $orders) {\n total\n number\n size\n overallBuyerVolume\n overallSellerVolume\n __typename\n  }\n}\n"",
                ""variables"": {
                                ""size"": 20,
                    ""number"": 0,
                    ""filter"": null
                },
                ""operationName"": ""SearchReportWoodDealCount""
                }";

                // Создаем тело запроса
                HttpContent content = new StringContent(jsonBody, System.Text.Encoding.UTF8, "application/json");

                // Отправляем запрос POST на сервер
                HttpResponseMessage response = client.PostAsync(RequestUrl, content).Result;

                // Обрабатываем ответ
                if (response.IsSuccessStatusCode)
                {
                    // Считываем тело ответа как строку
                    string responseContent = await response.Content.ReadAsStringAsync();

                    // Создаем объект JSON из строки ответа
                    JObject responseJObject = JObject.Parse(responseContent);

                    return Convert.ToInt32(responseJObject["data"]["searchReportWoodDeal"]["total"]);
                }
                else
                {
                    Console.WriteLine("Ошибка при выполнении запроса: " + response.StatusCode);
                    return -1;
                }
            }
        }

        private static async Task<List<Deal>> GetDealsFromServerAsync(int size, int number)
        {
            using (HttpClient client = new HttpClient())
            {
                // Задаем url для запросов
                const string RequestUrl = "https://www.lesegais.ru/open-area/graphql";

                // Устанавливаем заголовки
                client.DefaultRequestHeaders.Add("Host", "www.lesegais.ru");
                client.DefaultRequestHeaders.Add("User-Agent", "Chrome/108.0.0.0");

                // Устанавливаем заполнение тела запроса
                string jsonBody =
                @"{
                ""query"": ""query SearchReportWoodDeal($size: Int!, $number: Int!, $filter: Filter, $orders: [Order!]) {\n searchReportWoodDeal(filter: $filter, pageable: { number: $number, size: $size}, orders: $orders) {\n content {\n sellerName\n sellerInn\n buyerName\n buyerInn\n woodVolumeBuyer\n woodVolumeSeller\n dealDate\n dealNumber\n __typename\n    }\n __typename\n  }\n}\n"",
                ""variables"": {
                            ""size"": " + size + @",
                    ""number"": " + number + @",
                    ""filter"": null,
                    ""orders"": null
                },
                ""operationName"": ""SearchReportWoodDeal""
                }";

                // Создаем тело запроса
                HttpContent content = new StringContent(jsonBody, System.Text.Encoding.UTF8, "application/json");

                // Отправляем запрос POST на сервер
                HttpResponseMessage response = client.PostAsync(RequestUrl, content).Result;

                // Обрабатываем ответ
                if (response.IsSuccessStatusCode)
                {
                    // Считываем тело ответа как строку
                    string responseContent = await response.Content.ReadAsStringAsync();

                    // Вычленяем нужный участок JSON
                    JObject responseJObject = JObject.Parse(responseContent);
                    var dealsFromSite = responseJObject["data"]["searchReportWoodDeal"]["content"].Children().ToList();
                    var returnDeals = new List<Deal>();

                    // Заполняем коллекцию для отправки в БД
                    foreach (var deal in dealsFromSite)
                    {
                        Deal returnDeal = deal.ToObject<Deal>(new JsonSerializer
                        {
                            DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate,
                            NullValueHandling = NullValueHandling.Ignore
                        });
                        returnDeals.Add(returnDeal);
                    }

                    return returnDeals;
                }
                else
                {
                    Console.WriteLine("Ошибка при выполнении запроса: " + response.StatusCode);
                    return null;
                }
            }
        }

        private static void SaveDealsToDatabase(IEnumerable<Deal> deals, string connectionString)
        {
            // Создаем подключение к базе данных
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                // Создаем команду для получения списков ID сделок
                List<string> dealIds = new List<string>();
                SqlCommand checkCommand = new SqlCommand("SELECT dealNumber FROM Deals", connection);

                // Открываем соединение
                connection.Open();

                // Сохраняем список ID в БД в виде списка
                using (SqlDataReader reader = checkCommand.ExecuteReader())
                {
                    while (reader.Read())
                        dealIds.Add(reader.GetString(0));
                }

                deals = deals.GroupBy(d => d.dealNumber) // группируем сделки по dealNumber
                .Select(g => g.OrderByDescending(d => d.dealDate).First())
                .Where(deal => deal.isValid()) // выбираем первую сделку с максимальным значением dealDate в каждой группе
                .ToList();

                // Создаем список из тех элементов, что уже присутствуют в БД
                var existDeals = deals.Where(deal => dealIds.Contains(deal.dealNumber)).ToList();

                // Создаем список из тех элементов, что еще не присутствуют в БД
                var notexistDeals = deals.Except(existDeals).ToList();

                // Создаем транзакцию для группировки операций вставки/обновления записей
                SqlTransaction transaction = connection.BeginTransaction();
                try
                {
                    // Выполняем вставку/обновление записей в транзакции
                    AddExists(existDeals, connection, transaction);
                    AddNotExists(notexistDeals, connection, transaction);

                    // Если все операции выполнены успешно, то коммитим транзакцию
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    // Если возникла ошибка, то откатываем транзакцию и выбрасываем исключение
                    transaction.Rollback();
                    throw new Exception("Ошибка сохранения данных в БД", ex);
                }
            }
        }

        private static void AddNotExists(List<Deal> deals, SqlConnection connection, SqlTransaction transaction)
        {
            // Создаем команду для вставки записи в таблицу Deals
            SqlCommand insertCommand = new SqlCommand(@"INSERT INTO Deals 
                (sellerName, sellerInn, buyerName, buyerInn, woodVolumeBuyer, woodVolumeSeller, dealDate, dealNumber) 
                VALUES (@sellerName, @sellerInn, @buyerName, @buyerInn, @woodVolumeBuyer, @woodVolumeSeller, @dealDate, @dealNumber)", connection);
            insertCommand.Parameters.AddWithValue("@sellerName", "");
            insertCommand.Parameters.AddWithValue("@sellerInn", "");
            insertCommand.Parameters.AddWithValue("@buyerName", "");
            insertCommand.Parameters.AddWithValue("@buyerInn", "");
            insertCommand.Parameters.AddWithValue("@woodVolumeBuyer", 0);
            insertCommand.Parameters.AddWithValue("@woodVolumeSeller", 0);
            insertCommand.Parameters.AddWithValue("@dealDate", DateTime.Now);
            insertCommand.Parameters.AddWithValue("@dealNumber", "");
            insertCommand.Transaction = transaction;

            foreach (Deal deal in deals)
            {
                insertCommand.Parameters["@sellerName"].Value = deal.sellerName;
                insertCommand.Parameters["@sellerInn"].Value = deal.sellerInn;
                insertCommand.Parameters["@buyerName"].Value = deal.buyerName;
                insertCommand.Parameters["@buyerInn"].Value = deal.buyerInn;
                insertCommand.Parameters["@woodVolumeBuyer"].Value = deal.woodVolumeBuyer;
                insertCommand.Parameters["@woodVolumeSeller"].Value = deal.woodVolumeSeller;
                insertCommand.Parameters["@dealDate"].Value = deal.dealDate;
                insertCommand.Parameters["@dealNumber"].Value = deal.dealNumber;

                insertCommand.ExecuteNonQuery();
            }
        }

        private static void AddExists(List<Deal> deals, SqlConnection connection, SqlTransaction transaction)
        {
            // Создаем команду для обновления записи в таблице Deals
            SqlCommand updateCommand = new SqlCommand(@"UPDATE Deals SET sellerName = @sellerName, sellerInn = @sellerInn, buyerName = @buyerName, 
                buyerInn = @buyerInn, woodVolumeBuyer = @woodVolumeBuyer, woodVolumeSeller = @woodVolumeSeller, dealDate = @dealDate 
                WHERE dealNumber = @dealNumber", connection);
            updateCommand.Parameters.AddWithValue("@sellerName", "");
            updateCommand.Parameters.AddWithValue("@sellerInn", "");
            updateCommand.Parameters.AddWithValue("@buyerName", "");
            updateCommand.Parameters.AddWithValue("@buyerInn", "");
            updateCommand.Parameters.AddWithValue("@woodVolumeBuyer", 0);
            updateCommand.Parameters.AddWithValue("@woodVolumeSeller", 0);
            updateCommand.Parameters.AddWithValue("@dealDate", DateTime.Now);
            updateCommand.Parameters.AddWithValue("@dealNumber", "");
            updateCommand.Transaction = transaction;

            SqlCommand selectCommand = new SqlCommand("SELECT sellerName, sellerInn, buyerName, buyerInn, woodVolumeBuyer, woodVolumeSeller, dealDate FROM Deals WHERE dealNumber = @dealNumber", connection);
            selectCommand.Parameters.AddWithValue("@dealNumber", "");
            selectCommand.Transaction = transaction;

            foreach (Deal deal in deals)
            {
                // Если запись уже существует, то проверяем, отличаются ли данные
                selectCommand.Parameters["@dealNumber"].Value = deal.dealNumber;

                Deal dealDB = new Deal();

                using (SqlDataReader reader = selectCommand.ExecuteReader())
                {
                    reader.Read();

                    dealDB.sellerName = reader.GetString(0);
                    dealDB.sellerInn = reader.GetString(1);
                    dealDB.buyerName = reader.GetString(2);
                    dealDB.buyerInn = reader.GetString(3);
                    dealDB.woodVolumeBuyer = reader.GetDecimal(4);
                    dealDB.woodVolumeSeller = reader.GetDecimal(5);
                    dealDB.dealDate = reader.GetDateTime(6);
                }
                if (dealDB.sellerName != deal.sellerName ||
                    dealDB.sellerInn.Trim() != deal.sellerInn ||
                    dealDB.buyerName != deal.buyerName ||
                    dealDB.buyerInn.Trim() != deal.buyerInn ||
                    dealDB.woodVolumeBuyer != deal.woodVolumeBuyer ||
                    dealDB.woodVolumeSeller != deal.woodVolumeSeller ||
                    dealDB.dealDate != deal.dealDate)
                {
                    // Если данные отличаются, то обновляем запись
                    updateCommand.Parameters["@sellerName"].Value = deal.sellerName;
                    updateCommand.Parameters["@sellerInn"].Value = deal.sellerInn;
                    updateCommand.Parameters["@buyerName"].Value = deal.buyerName;
                    updateCommand.Parameters["@buyerInn"].Value = deal.buyerInn;
                    updateCommand.Parameters["@woodVolumeBuyer"].Value = deal.woodVolumeBuyer;
                    updateCommand.Parameters["@woodVolumeSeller"].Value = deal.woodVolumeSeller;
                    updateCommand.Parameters["@dealDate"].Value = deal.dealDate;
                    updateCommand.Parameters["@dealNumber"].Value = deal.dealNumber;

                    updateCommand.ExecuteNonQuery();
                }
            }
        }
    }
}