using System.Data.SqlClient;

class Program
{
    static void Main()
    {
        string eskiDbConnectionString = "Server=CIGDEM;Database=EskiDb;Integrated Security=True;";
        string yeniDbConnectionString = "Server=CIGDEM;Database=YeniDb;Integrated Security=True;";

        using (SqlConnection eskiDbConnection = new SqlConnection(eskiDbConnectionString))
        using (SqlConnection yeniDbConnection = new SqlConnection(yeniDbConnectionString))
        {
            eskiDbConnection.Open();
            yeniDbConnection.Open();

            var eskiSicilList = GetSicilList(eskiDbConnection);
            var yeniSicilDict = GetSicilDictionary(yeniDbConnection);
            var poolList = GetPoolList(eskiDbConnection);

            int TekSicilId = 678;
            var belirliSicilPoolList = GetPoolListBySicilId(eskiDbConnection, TekSicilId);



            #region
            //foreach (var eskiSicil in eskiSicilList)
            //{
            //    if (yeniSicilDict.TryGetValue(eskiSicil.PersonelNo, out List<int> yeniSicilIds))
            //    {
            //        int yeniSicilId = yeniSicilIds.FirstOrDefault();  // İlk ID'yi baz alıyor.

            //        var poolItems = poolList.Where(x => x.SicilID == eskiSicil.Id).ToList();

            //        if (poolItems.Any())
            //        {
            //            string yeniUserId = yeniSicilId.ToString().PadLeft(8, '0');

            //            foreach (var poolItem in poolItems)
            //            {
            //                string updateQuery = "UPDATE Pool SET SicilID = @YeniSicilId, UserID = @YeniUserId WHERE ID = @PoolId";
            //                using (var updateCmd = new SqlCommand(updateQuery, eskiDbConnection))
            //                {
            //                    updateCmd.Parameters.AddWithValue("@YeniSicilId", yeniSicilId);
            //                    updateCmd.Parameters.AddWithValue("@YeniUserId", yeniUserId);
            //                    updateCmd.Parameters.AddWithValue("@PoolId", poolItem.PoolId);

            //                    try
            //                    {
            //                        int affectedRows = updateCmd.ExecuteNonQuery();
            //                        Console.WriteLine($"Güncellenen satır sayısı: {affectedRows} (Pool ID: {poolItem.PoolId})");
            //                    }
            //                    catch (SqlException ex)
            //                    {
            //                        Console.WriteLine($"Hata: {ex.Message} (Pool ID: {poolItem.PoolId})");
            //                    }
            //                }
            //            }
            //        }
            //        else
            //        {
            //            Console.WriteLine($"Pool tablosunda Sicil bulunamadı. SicilID: {eskiSicil.Id}");
            //        }
            //    }
            //    else
            //    {
            //        Console.WriteLine($"Yeni Sicil bulunamadı. PersonelNo: {eskiSicil.PersonelNo}");
            //    }
            //}

            #endregion



            var i = 0;
            var allPoolItems = GetAllPoolItems(eskiDbConnection);
            foreach (var poolItem in allPoolItems)

            {
                int? terminalId = poolItem.TerminalID;
                if (poolItem.PDKS == 1)
                {
                    terminalId = 11;
                }
                else if (poolItem.PDKS == 2)
                {
                    terminalId = 10;
                }


                string insertQuery = "INSERT INTO Pool (SicilID, UserID, TerminalID, EventTime, EventCode, FuncCode, Automatic, Deleted, PDKS, ReaderID, Status, UnDelete, ForeignID, pdksx, TIP) " +
                                     "VALUES (@SicilId, @UserId, @TerminalID, @EventTime, @EventCode, @FuncCode, @Automatic, @Deleted, @PDKS, @ReaderID, @Status, @UnDelete, @ForeignID, @pdksx, @TIP)";
                using (var insertCmd = new SqlCommand(insertQuery, yeniDbConnection))
                {
                    insertCmd.Parameters.AddWithValue("@SicilId", poolItem.SicilID is null ? DBNull.Value : poolItem.SicilID);
                    insertCmd.Parameters.AddWithValue("@UserId", poolItem.UserID is null ? DBNull.Value : poolItem.UserID);
                    insertCmd.Parameters.AddWithValue("@TerminalID", poolItem.TerminalID is null ? DBNull.Value : poolItem.TerminalID);
                    insertCmd.Parameters.AddWithValue("@EventTime", poolItem.EventTime is null ? DBNull.Value : poolItem.EventTime);
                    insertCmd.Parameters.AddWithValue("@EventCode", poolItem.EventCode is null ? DBNull.Value : poolItem.EventCode);
                    insertCmd.Parameters.AddWithValue("@FuncCode", poolItem.FuncCode is null ? DBNull.Value : poolItem.FuncCode);
                    insertCmd.Parameters.AddWithValue("@Automatic", poolItem.Automatic is null ? DBNull.Value : poolItem.Automatic);
                    insertCmd.Parameters.AddWithValue("@Deleted", poolItem.Deleted is null ? DBNull.Value : poolItem.Deleted);
                    insertCmd.Parameters.AddWithValue("@PDKS", poolItem.PDKS is null ? DBNull.Value : poolItem.PDKS);
                    insertCmd.Parameters.AddWithValue("@ReaderID", poolItem.ReaderID is null ? DBNull.Value : poolItem.ReaderID);
                    insertCmd.Parameters.AddWithValue("@Status", poolItem.Status is null ? DBNull.Value : poolItem.Status);
                    insertCmd.Parameters.AddWithValue("@UnDelete", poolItem.UnDelete is null ? DBNull.Value : poolItem.UnDelete);
                    insertCmd.Parameters.AddWithValue("@ForeignID", poolItem.ForeignID is null ? DBNull.Value : poolItem.ForeignID);
                    insertCmd.Parameters.AddWithValue("@pdksx", poolItem.pdksx is null ? DBNull.Value : poolItem.pdksx);
                    insertCmd.Parameters.AddWithValue("@TIP", poolItem.TIP is null ? DBNull.Value : poolItem.TIP);

                    try
                    {
                        int affectedRows = insertCmd.ExecuteNonQuery();
                        Console.WriteLine($"Yeni satır eklendi. (SicilID: {poolItem.SicilID}, UserID: {poolItem.UserID})");
                        i++;
                    }
                    catch (SqlException ex)
                    {
                        Console.WriteLine($"Hata: {ex.Message} (SicilID: {poolItem.SicilID})");
                    }
                    Console.WriteLine(i);
                }
            }

            Console.WriteLine("Güncelleme ve ekleme işlemi tamamlandı.");
        }
    }






    public static List<(int Id, string PersonelNo)> GetSicilList(SqlConnection connection)
    {
        string query = "SELECT ID, PersonelNo FROM Sicil";
        var sicilList = new List<(int Id, string PersonelNo)>();

        using (var cmd = new SqlCommand(query, connection))
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                sicilList.Add((reader.GetInt32(0), reader.GetString(1)));
            }
        }
        return sicilList;
    }



    public static Dictionary<string, List<int>> GetSicilDictionary(SqlConnection connection)
    {
        string query = "SELECT ID, PersonelNo FROM Sicil";
        var sicilDict = new Dictionary<string, List<int>>();

        using (var cmd = new SqlCommand(query, connection))
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                string personelNo = reader.GetString(1);
                int id = reader.GetInt32(0);

                if (!sicilDict.ContainsKey(personelNo))
                {
                    sicilDict[personelNo] = new List<int>();
                }
                sicilDict[personelNo].Add(id);
            }
        }
        return sicilDict;
    }

    public static List<(int PoolId, int? SicilID, string? UserID, int? TerminalID, DateTime? EventTime, string? EventCode, int? FuncCode, bool? Automatic, int? Deleted, int? PDKS, int? ReaderID, string? Status, bool? UnDelete, long? ForeignID, int? pdksx, string? TIP)> GetAllPoolItems(SqlConnection connection)
    {

        string query = "SELECT ID, SicilID, UserID, TerminalID, EventTime, EventCode, FuncCode, Automatic, Deleted, PDKS, ReaderID, Status, UnDelete, ForeignID, pdksx, TIP FROM Pool";
        //string query = "SELECT * \r\nFROM [EskiDb].[dbo].[Pool] e\r\nWHERE  NOT EXISTS (\r\n    SELECT 1\r\n    FROM [YeniDb].[dbo].[Pool] y\r\n    WHERE y.SicilID = e.SicilID\r\n    AND y.EventTime = e.EventTime\r\n);\r\n";
        var poolItems = new List<(int PoolId, int? SicilID, string? UserID, int? TerminalID, DateTime? EventTime, string? EventCode, int? FuncCode, bool? Automatic, int? Deleted, int? PDKS, int? ReaderID, string? Status, bool? UnDelete, long? ForeignID, int? pdksx, string? TIP)>();


        using (var cmd = new SqlCommand(query, connection))
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                poolItems.Add((
                reader.GetInt32(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3),
                reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? (int?)null : reader.GetInt32(6),
                reader.IsDBNull(7) ? (bool?)null : reader.GetBoolean(7),
                reader.IsDBNull(8) ? (int?)null : reader.GetInt32(8),
                reader.IsDBNull(9) ? (int?)null : reader.GetInt32(9),
                reader.IsDBNull(10) ? (int?)null : reader.GetInt32(10),
                reader.IsDBNull(11) ? null : reader.GetString(11),
                reader.IsDBNull(12) ? (bool?)null : reader.GetBoolean(12),
                reader.IsDBNull(13) ? (long?)null : reader.GetInt64(13),
                reader.IsDBNull(14) ? (int?)null : reader.GetInt32(14),
                reader.IsDBNull(15) ? null : reader.GetString(15)
            ));


            }
        }
        return poolItems;
    }


    public static List<(int PoolId, int SicilID, string? UserID, int TerminalID, DateTime EventTime, string EventCode, int FuncCode, bool Automatic, int Deleted, int PDKS, int ReaderID, string? Status, bool UnDelete, long ForeignID, int pdksx, string? TIP)> GetPoolList(SqlConnection connection)
    {
        string query = "SELECT ID, SicilID, UserID, TerminalID, EventTime, EventCode, FuncCode, Automatic, Deleted, PDKS, ReaderID, Status, UnDelete, ForeignID, pdksx, TIP FROM Pool";
        var poolItems = new List<(int PoolId, int SicilID, string? UserID, int TerminalID, DateTime EventTime, string EventCode, int FuncCode, bool Automatic, int Deleted, int PDKS, int ReaderID, string? Status, bool UnDelete, long ForeignID, int pdksx, string? TIP)>();

        using (var cmd = new SqlCommand(query, connection))
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                poolItems.Add((reader.GetInt32(0),
                                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                                reader.IsDBNull(2) ? null : reader.GetString(2),
                                reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                                reader.GetDateTime(4),
                                reader.GetString(5),
                                reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                                reader.GetBoolean(7),
                                reader.GetInt32(8),
                                reader.GetInt32(9),
                                reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                                reader.IsDBNull(11) ? null : reader.GetString(11),
                                reader.GetBoolean(12),
                                reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                                reader.IsDBNull(14) ? 0 : reader.GetInt32(14),
                                reader.IsDBNull(15) ? null : reader.GetString(15)));
            }

        }
        return poolItems;
    }




    public static List<(int PoolId, int SicilID, string? UserID, int TerminalID, DateTime? EventTime, string EventCode, int FuncCode, bool Automatic, int Deleted, int PDKS, int ReaderID, string? Status, bool UnDelete, long ForeignID, int pdksx, string? TIP)> GetPoolListBySicilId(SqlConnection connection, int sicilId)
    {
        string query = "SELECT ID, SicilID, UserID, TerminalID, EventTime, EventCode, FuncCode, Automatic, Deleted, PDKS, ReaderID, Status, UnDelete, ForeignID, pdksx, TIP " +
                       "FROM Pool WHERE SicilID = @SicilID";
        var poolItems = new List<(int PoolId, int SicilID, string? UserID, int TerminalID, DateTime? EventTime, string EventCode, int FuncCode, bool Automatic, int Deleted, int PDKS, int ReaderID, string? Status, bool UnDelete, long ForeignID, int pdksx, string? TIP)>();

        using (var cmd = new SqlCommand(query, connection))
        {
            cmd.Parameters.AddWithValue("@SicilID", sicilId);
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    poolItems.Add((
                        reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                        reader.GetInt32(1),
                        reader.IsDBNull(2) ? null : reader.GetString(2),
                        reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                        reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4),
                        reader.GetString(5),
                        reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                        reader.GetBoolean(7),
                        reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
                        reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                        reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                        reader.IsDBNull(11) ? null : reader.GetString(11),
                        reader.GetBoolean(12),
                        reader.GetInt64(13),
                        reader.IsDBNull(14) ? 0 : reader.GetInt32(14),
                        reader.IsDBNull(15) ? null : reader.GetString(15)
                    ));
                }
            }
        }

        return poolItems;
    }




    public static void AddPoolItemsBySicilId(SqlConnection eskiDbConnection, SqlConnection yeniDbConnection, int sicilId)
    {
        string selectQuery = "SELECT ID, SicilID, UserID, TerminalID, EventTime, EventCode, FuncCode, Automatic, Deleted, PDKS, ReaderID, Status, UnDelete, ForeignID, pdksx, TIP FROM Pool WHERE SicilID = @SicilId AND PDKS != 0";

        var poolItems = new List<(int PoolId, int? SicilID, string? UserID, int? TerminalID, DateTime? EventTime, string? EventCode, int? FuncCode, bool? Automatic, int? Deleted, int? PDKS, int? ReaderID, string? Status, bool? UnDelete, long? ForeignID, int? pdksx, string? TIP)>();

        using (var selectCmd = new SqlCommand(selectQuery, eskiDbConnection))
        {
            selectCmd.Parameters.AddWithValue("@SicilId", sicilId);
            using (var reader = selectCmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    poolItems.Add((
                        reader.GetInt32(0),
                        reader.IsDBNull(1) ? null : reader.GetInt32(1),
                        reader.IsDBNull(2) ? null : reader.GetString(2),
                        reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3),
                        reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4),
                        reader.IsDBNull(5) ? null : reader.GetString(5),
                        reader.IsDBNull(6) ? (int?)null : reader.GetInt32(6),
                        reader.IsDBNull(7) ? (bool?)null : reader.GetBoolean(7),
                        reader.IsDBNull(8) ? (int?)null : reader.GetInt32(8),
                        reader.IsDBNull(9) ? (int?)null : reader.GetInt32(9),
                        reader.IsDBNull(10) ? (int?)null : reader.GetInt32(10),
                        reader.IsDBNull(11) ? null : reader.GetString(11),
                        reader.IsDBNull(12) ? (bool?)null : reader.GetBoolean(12),
                        reader.IsDBNull(13) ? (long?)null : reader.GetInt64(13),
                        reader.IsDBNull(14) ? (int?)null : reader.GetInt32(14),
                        reader.IsDBNull(15) ? null : reader.GetString(15)
                    ));
                }
            }
        }

        foreach (var poolItem in poolItems)
        {
            string insertQuery = "INSERT INTO Pool (SicilID, UserID, TerminalID, EventTime, EventCode, FuncCode, Automatic, Deleted, PDKS, ReaderID, Status, UnDelete, ForeignID, pdksx, TIP) " +
                                 "VALUES (@SicilId, @UserId, @TerminalID, @EventTime, @EventCode, @FuncCode, @Automatic, @Deleted, @PDKS, @ReaderID, @Status, @UnDelete, @ForeignID, @pdksx, @TIP)";

            using (var insertCmd = new SqlCommand(insertQuery, yeniDbConnection))
            {
                insertCmd.Parameters.AddWithValue("@SicilId", poolItem.SicilID ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@UserId", poolItem.UserID ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@TerminalID", poolItem.TerminalID ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@EventTime", poolItem.EventTime ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@EventCode", poolItem.EventCode ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@FuncCode", poolItem.FuncCode ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@Automatic", poolItem.Automatic ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@Deleted", poolItem.Deleted ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@PDKS", poolItem.PDKS ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@ReaderID", poolItem.ReaderID ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@Status", poolItem.Status ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@UnDelete", poolItem.UnDelete ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@ForeignID", poolItem.ForeignID ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@pdksx", poolItem.pdksx ?? (object)DBNull.Value);
                insertCmd.Parameters.AddWithValue("@TIP", poolItem.TIP ?? (object)DBNull.Value);

                try
                {
                    int affectedRows = insertCmd.ExecuteNonQuery();
                    Console.WriteLine($"Yeni satır eklendi. (SicilID: {poolItem.SicilID}, UserID: {poolItem.UserID})");
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Hata: {ex.Message} (SicilID: {poolItem.SicilID})");
                }
            }
        }
    }
}







